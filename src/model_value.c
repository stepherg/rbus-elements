#include "model_value.h"
#include "model_alloc.h"

#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <stdarg.h>

static bool fail(char* error, size_t error_size, const char* format, ...) {
   if (error && error_size > 0) {
      va_list args;
      va_start(args, format);
      vsnprintf(error, error_size, format, args);
      va_end(args);
   }
   return false;
}

static bool is_integer_text(const char* text, bool allow_negative) {
   if (!text || !*text) return false;
   if (*text == '-') {
      if (!allow_negative || !*++text) return false;
   }
   if (*text == '0' && text[1] != '\0') return false;
   while (*text) {
      if (!isdigit((unsigned char)*text++)) return false;
   }
   return true;
}

static bool is_real_text(const char* text) {
   if (!text || !*text) return false;
   if (*text == '-' && !*++text) return false;
   if (*text == '0') {
      text++;
      if (isdigit((unsigned char)*text)) return false;
   } else {
      if (*text < '1' || *text > '9') return false;
      while (isdigit((unsigned char)*text)) text++;
   }
   if (*text == '.') {
      text++;
      if (!isdigit((unsigned char)*text)) return false;
      while (isdigit((unsigned char)*text)) text++;
   }
   if (*text == 'e' || *text == 'E') {
      text++;
      if (*text == '+' || *text == '-') text++;
      if (!isdigit((unsigned char)*text)) return false;
      while (isdigit((unsigned char)*text)) text++;
   }
   return *text == '\0';
}

static bool parse_signed(const cJSON* json, int64_t minimum, int64_t maximum,
   int64_t* result, char* error, size_t error_size) {
   if (cJSON_IsString(json)) {
      const char* text = cJSON_GetStringValue(json);
      if (!is_integer_text(text, true)) return fail(error, error_size, "expected canonical signed integer");
      errno = 0;
      char* end = NULL;
      intmax_t parsed = strtoimax(text, &end, 10);
      if (errno == ERANGE || !end || *end || parsed < minimum || parsed > maximum)
         return fail(error, error_size, "signed integer out of range");
      *result = (int64_t)parsed;
      return true;
   }
   if (!cJSON_IsNumber(json) || !isfinite(json->valuedouble) || trunc(json->valuedouble) != json->valuedouble)
      return fail(error, error_size, "expected integral JSON number or canonical string");
   double exact_minimum = minimum < -9007199254740991LL ? -9007199254740991.0 : (double)minimum;
   double exact_maximum = maximum > 9007199254740991LL ? 9007199254740991.0 : (double)maximum;
   if (json->valuedouble < exact_minimum || json->valuedouble > exact_maximum)
      return fail(error, error_size, "signed integer out of range");
   *result = (int64_t)json->valuedouble;
   return true;
}

static bool parse_unsigned(const cJSON* json, uint64_t maximum, uint64_t* result,
   char* error, size_t error_size) {
   if (cJSON_IsString(json)) {
      const char* text = cJSON_GetStringValue(json);
      if (!is_integer_text(text, false)) return fail(error, error_size, "expected canonical unsigned integer");
      errno = 0;
      char* end = NULL;
      uintmax_t parsed = strtoumax(text, &end, 10);
      if (errno == ERANGE || !end || *end || parsed > maximum)
         return fail(error, error_size, "unsigned integer out of range");
      *result = (uint64_t)parsed;
      return true;
   }
   if (!cJSON_IsNumber(json) || !isfinite(json->valuedouble) || trunc(json->valuedouble) != json->valuedouble)
      return fail(error, error_size, "expected integral JSON number or canonical string");
   double exact_maximum = maximum > 9007199254740991ULL ? 9007199254740991.0 : (double)maximum;
   if (json->valuedouble < 0 || json->valuedouble > exact_maximum)
      return fail(error, error_size, "unsigned integer out of range");
   *result = (uint64_t)json->valuedouble;
   return true;
}

static bool parse_real(const cJSON* json, bool single, double* result,
   char* error, size_t error_size) {
   double parsed;
   if (cJSON_IsNumber(json)) {
      parsed = json->valuedouble;
   } else if (cJSON_IsString(json)) {
      const char* text = cJSON_GetStringValue(json);
      if (!is_real_text(text)) return fail(error, error_size, "expected canonical real number");
      errno = 0;
      char* end = NULL;
      parsed = strtod(text, &end);
      if (errno == ERANGE || !end || *end) return fail(error, error_size, "invalid real number");
   } else {
      return fail(error, error_size, "expected JSON number or canonical string");
   }
   if (!isfinite(parsed) || (single && (parsed > FLT_MAX || parsed < -FLT_MAX)))
      return fail(error, error_size, "real number out of range");
   *result = parsed;
   return true;
}

bool model_value_parse(const cJSON* json, ValueType type, ElementValue* value,
   char* error, size_t error_size) {
   if (!json || !value) return fail(error, error_size, "missing value");
   memset(value, 0, sizeof(*value));

   int64_t signed_value;
   uint64_t unsigned_value;
   double real_value;
   switch (type) {
      case TYPE_STRING:
      case TYPE_DATETIME:
      case TYPE_BASE64:
         if (!cJSON_IsString(json)) return fail(error, error_size, "expected string");
         value->strVal = model_strdup(cJSON_GetStringValue(json));
         return value->strVal ? true : fail(error, error_size, "out of memory");
      case TYPE_INT:
         if (!parse_signed(json, INT32_MIN, INT32_MAX, &signed_value, error, error_size)) return false;
         value->intVal = (int32_t)signed_value;
         return true;
      case TYPE_UINT:
         if (!parse_unsigned(json, UINT32_MAX, &unsigned_value, error, error_size)) return false;
         value->uintVal = (uint32_t)unsigned_value;
         return true;
      case TYPE_BOOL:
         if (cJSON_IsBool(json)) {
            value->boolVal = cJSON_IsTrue(json);
            return true;
         }
         if (cJSON_IsString(json) && strcmp(cJSON_GetStringValue(json), "true") == 0) {
            value->boolVal = true;
            return true;
         }
         if (cJSON_IsString(json) && strcmp(cJSON_GetStringValue(json), "false") == 0) {
            value->boolVal = false;
            return true;
         }
         return fail(error, error_size, "expected boolean or canonical boolean string");
      case TYPE_LONG:
         if (!parse_signed(json, INT64_MIN, INT64_MAX, &signed_value, error, error_size)) return false;
         value->longVal = signed_value;
         return true;
      case TYPE_ULONG:
         if (!parse_unsigned(json, UINT64_MAX, &unsigned_value, error, error_size)) return false;
         value->ulongVal = unsigned_value;
         return true;
      case TYPE_FLOAT:
         if (!parse_real(json, true, &real_value, error, error_size)) return false;
         value->floatVal = (float)real_value;
         return true;
      case TYPE_DOUBLE:
         if (!parse_real(json, false, &real_value, error, error_size)) return false;
         value->doubleVal = real_value;
         return true;
      case TYPE_BYTE:
         if (!parse_unsigned(json, UINT8_MAX, &unsigned_value, error, error_size)) return false;
         value->byteVal = (uint8_t)unsigned_value;
         return true;
   }
   return fail(error, error_size, "unsupported value type %d", type);
}

void model_value_free(ValueType type, ElementValue* value) {
   if (value && IS_STRING_TYPE(type)) {
      free(value->strVal);
      value->strVal = NULL;
   }
}