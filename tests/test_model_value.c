#include "test_model_value.h"

#include "model_value.h"
#include "test_support.h"

#include <math.h>

static bool parse(const char* json_text, ValueType type, ElementValue* value) {
   cJSON* json = cJSON_Parse(json_text);
   char error[128];
   bool result = json && model_value_parse(json, type, value, error, sizeof(error));
   cJSON_Delete(json);
   return result;
}

static bool rejects(const char* json_text, ValueType type) {
   ElementValue value;
   return !parse(json_text, type, &value);
}

bool test_model_values(void) {
   ElementValue value;
   TEST_ASSERT(parse("\"text\"", TYPE_STRING, &value));
   TEST_ASSERT(strcmp(value.strVal, "text") == 0);
   model_value_free(TYPE_STRING, &value);
   TEST_ASSERT(parse("\"2025-01-01T00:00:00Z\"", TYPE_DATETIME, &value));
   model_value_free(TYPE_DATETIME, &value);
   TEST_ASSERT(parse("\"YWJj\"", TYPE_BASE64, &value));
   model_value_free(TYPE_BASE64, &value);

   TEST_ASSERT(parse("100", TYPE_INT, &value) && value.intVal == 100);
   TEST_ASSERT(parse("\"100\"", TYPE_INT, &value) && value.intVal == 100);
   TEST_ASSERT(parse("100", TYPE_UINT, &value) && value.uintVal == 100);
   TEST_ASSERT(parse("\"100\"", TYPE_UINT, &value) && value.uintVal == 100);
   TEST_ASSERT(parse("true", TYPE_BOOL, &value) && value.boolVal);
   TEST_ASSERT(parse("\"true\"", TYPE_BOOL, &value) && value.boolVal);
   TEST_ASSERT(parse("-9007199254740991", TYPE_LONG, &value) && value.longVal == -9007199254740991LL);
   TEST_ASSERT(parse("\"-9223372036854775808\"", TYPE_LONG, &value) && value.longVal == INT64_MIN);
   TEST_ASSERT(parse("9007199254740991", TYPE_ULONG, &value) && value.ulongVal == 9007199254740991ULL);
   TEST_ASSERT(parse("\"18446744073709551615\"", TYPE_ULONG, &value) && value.ulongVal == UINT64_MAX);
   TEST_ASSERT(parse("1.25", TYPE_FLOAT, &value) && fabsf(value.floatVal - 1.25f) < 0.001f);
   TEST_ASSERT(parse("\"1.25\"", TYPE_FLOAT, &value) && fabsf(value.floatVal - 1.25f) < 0.001f);
   TEST_ASSERT(parse("1.25", TYPE_DOUBLE, &value) && fabs(value.doubleVal - 1.25) < 0.000001);
   TEST_ASSERT(parse("\"1.25\"", TYPE_DOUBLE, &value) && fabs(value.doubleVal - 1.25) < 0.000001);
   TEST_ASSERT(parse("255", TYPE_BYTE, &value) && value.byteVal == 255);
   TEST_ASSERT(parse("\"255\"", TYPE_BYTE, &value) && value.byteVal == 255);

   TEST_ASSERT(rejects("null", TYPE_STRING));
   TEST_ASSERT(rejects("1.5", TYPE_INT));
   TEST_ASSERT(rejects("\"1.5\"", TYPE_UINT));
   TEST_ASSERT(rejects("256", TYPE_BYTE));
   TEST_ASSERT(rejects("\"2147483648\"", TYPE_INT));
   TEST_ASSERT(rejects("\"-1\"", TYPE_ULONG));
   TEST_ASSERT(rejects("\"True\"", TYPE_BOOL));
   TEST_ASSERT(rejects("\"01\"", TYPE_UINT));
   TEST_ASSERT(rejects("9223372036854775807", TYPE_LONG));
   TEST_ASSERT(rejects("18446744073709551615", TYPE_ULONG));
   TEST_ASSERT(rejects("\"01.0\"", TYPE_DOUBLE));
   TEST_ASSERT(rejects("\".5\"", TYPE_FLOAT));
   return true;
}