#include "psm_store.h"

#include <cJSON.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static bool fail(char* error, size_t error_size, const char* format, ...) {
   if (error && error_size) {
      va_list args;
      va_start(args, format);
      vsnprintf(error, error_size, format, args);
      va_end(args);
   }
   return false;
}

static const char* type_name(rbusValueType_t type) {
   switch (type) {
      case RBUS_BOOLEAN: return "boolean";
      case RBUS_BYTE: return "byte";
      case RBUS_INT8: return "int8";
      case RBUS_UINT8: return "uint8";
      case RBUS_INT16: return "int16";
      case RBUS_UINT16: return "uint16";
      case RBUS_INT32: return "int32";
      case RBUS_UINT32: return "uint32";
      case RBUS_INT64: return "int64";
      case RBUS_UINT64: return "uint64";
      case RBUS_SINGLE: return "single";
      case RBUS_DOUBLE: return "double";
      case RBUS_STRING: return "string";
      default: return NULL;
   }
}

static bool parse_type(const char* name, rbusValueType_t* type) {
   for (int candidate = RBUS_BOOLEAN; candidate <= RBUS_STRING; candidate++) {
      const char* known = type_name((rbusValueType_t)candidate);
      if (known && strcmp(name, known) == 0) {
         *type = (rbusValueType_t)candidate;
         return true;
      }
   }
   return false;
}

static PsmRecord* find_record(PsmRecord* records, const char* name) {
   for (PsmRecord* record = records; record; record = record->next) {
      if (strcmp(record->name, name) == 0) return record;
   }
   return NULL;
}

static void free_records(PsmRecord* records) {
   while (records) {
      PsmRecord* next = records->next;
      free(records->name);
      free(records->value);
      free(records);
      records = next;
   }
}

static bool append_record(PsmRecord** records, const char* name, rbusValueType_t type,
   const char* value, bool only_if_missing) {
   PsmRecord* existing = find_record(*records, name);
   if (existing && only_if_missing) return true;
   char* value_copy = strdup(value);
   if (!value_copy) return false;
   if (existing) {
      free(existing->value);
      existing->value = value_copy;
      existing->type = type;
      return true;
   }
   PsmRecord* record = calloc(1, sizeof(*record));
   if (!record) {
      free(value_copy);
      return false;
   }
   record->name = strdup(name);
   if (!record->name) {
      free(value_copy);
      free(record);
      return false;
   }
   record->type = type;
   record->value = value_copy;
   record->next = *records;
   *records = record;
   return true;
}

static bool read_file(const char* path, bool allow_missing, char** contents,
   char* error, size_t error_size) {
   FILE* file = fopen(path, "rb");
   if (!file) {
      if (allow_missing && errno == ENOENT) {
         *contents = NULL;
         return true;
      }
      return fail(error, error_size, "cannot open %s: %s", path, strerror(errno));
   }
   if (fseek(file, 0, SEEK_END) != 0) {
      fclose(file);
      return fail(error, error_size, "cannot seek %s", path);
   }
   long length = ftell(file);
   if (length < 0 || fseek(file, 0, SEEK_SET) != 0) {
      fclose(file);
      return fail(error, error_size, "cannot size %s", path);
   }
   char* data = malloc((size_t)length + 1);
   if (!data) {
      fclose(file);
      return fail(error, error_size, "out of memory reading %s", path);
   }
   size_t read = fread(data, 1, (size_t)length, file);
   bool complete = read == (size_t)length && !ferror(file);
   fclose(file);
   if (!complete) {
      free(data);
      return fail(error, error_size, "cannot read %s", path);
   }
   data[read] = '\0';
   *contents = data;
   return true;
}

static bool load_records(PsmRecord** records, const char* path, bool allow_missing,
   bool overlay, char* error, size_t error_size) {
   char* text = NULL;
   if (!read_file(path, allow_missing, &text, error, error_size)) return false;
   if (!text) return true;
   cJSON* root = cJSON_Parse(text);
   free(text);
   if (!root) return fail(error, error_size, "invalid JSON in %s", path);
   cJSON* version = cJSON_GetObjectItemCaseSensitive(root, "version");
   cJSON* json_records = cJSON_GetObjectItemCaseSensitive(root, "records");
   if (!cJSON_IsNumber(version) || version->valuedouble != 1 || !cJSON_IsObject(json_records)) {
      cJSON_Delete(root);
      return fail(error, error_size, "unsupported PSM schema in %s", path);
   }
   cJSON* item = NULL;
   cJSON_ArrayForEach(item, json_records) {
      cJSON* json_type = cJSON_GetObjectItemCaseSensitive(item, "type");
      cJSON* json_value = cJSON_GetObjectItemCaseSensitive(item, "value");
      rbusValueType_t type;
      if (!item->string || !*item->string || !cJSON_IsObject(item) || !cJSON_IsString(json_type) ||
         !cJSON_IsString(json_value) || !parse_type(cJSON_GetStringValue(json_type), &type) ||
         !append_record(records, item->string, type, cJSON_GetStringValue(json_value), overlay)) {
         cJSON_Delete(root);
         return fail(error, error_size, "invalid PSM record '%s' in %s", item->string ? item->string : "", path);
      }
      rbusValue_t check;
      rbusValue_Init(&check);
      bool valid = rbusValue_SetFromString(check, type, cJSON_GetStringValue(json_value));
      rbusValue_Release(check);
      if (!valid) {
         cJSON_Delete(root);
         return fail(error, error_size, "invalid typed PSM value '%s' in %s", item->string, path);
      }
   }
   cJSON_Delete(root);
   return true;
}

static char* serialize_records(PsmRecord* records, const PsmRecord* replacement) {
   cJSON* root = cJSON_CreateObject();
   cJSON* json_records = cJSON_CreateObject();
   if (!root || !json_records || !cJSON_AddNumberToObject(root, "version", 1) ||
      !cJSON_AddItemToObject(root, "records", json_records)) {
      cJSON_Delete(root);
      cJSON_Delete(json_records);
      return NULL;
   }
   bool replacement_written = false;
   for (PsmRecord* record = records; record; record = record->next) {
      const PsmRecord* output = replacement && strcmp(record->name, replacement->name) == 0 ? replacement : record;
      if (output == replacement) replacement_written = true;
      cJSON* item = cJSON_CreateObject();
      if (!item || !cJSON_AddStringToObject(item, "type", type_name(output->type)) ||
         !cJSON_AddStringToObject(item, "value", output->value) ||
         !cJSON_AddItemToObject(json_records, output->name, item)) {
         cJSON_Delete(item);
         cJSON_Delete(root);
         return NULL;
      }
   }
   if (replacement && !replacement_written) {
      cJSON* item = cJSON_CreateObject();
      if (!item || !cJSON_AddStringToObject(item, "type", type_name(replacement->type)) ||
         !cJSON_AddStringToObject(item, "value", replacement->value) ||
         !cJSON_AddItemToObject(json_records, replacement->name, item)) {
         cJSON_Delete(item);
         cJSON_Delete(root);
         return NULL;
      }
   }
   char* text = cJSON_Print(root);
   cJSON_Delete(root);
   return text;
}

static bool atomic_write(const char* path, const char* contents) {
   if (getenv("RBUS_ELEMENTS_PSM_FAIL_WRITE")) return false;
   size_t template_size = strlen(path) + sizeof(".tmp.XXXXXX");
   char* temporary = malloc(template_size);
   if (!temporary) return false;
   snprintf(temporary, template_size, "%s.tmp.XXXXXX", path);
   int descriptor = mkstemp(temporary);
   if (descriptor < 0 || fchmod(descriptor, S_IRUSR | S_IWUSR) != 0) {
      if (descriptor >= 0) close(descriptor);
      unlink(temporary);
      free(temporary);
      return false;
   }
   FILE* file = fdopen(descriptor, "w");
   bool success = file && fputs(contents, file) >= 0 && fflush(file) == 0 && fsync(descriptor) == 0;
   if (file) {
      if (fclose(file) != 0) success = false;
   } else {
      close(descriptor);
   }
   if (success && rename(temporary, path) == 0) {
      char* directory = strdup(path);
      char* slash = directory ? strrchr(directory, '/') : NULL;
      if (slash) *slash = '\0';
      int directory_descriptor = open(slash && directory[0] ? directory : ".", O_RDONLY | O_DIRECTORY);
      success = directory_descriptor >= 0 && fsync(directory_descriptor) == 0;
      if (directory_descriptor >= 0) close(directory_descriptor);
      free(directory);
   } else {
      success = false;
   }
   if (!success) unlink(temporary);
   free(temporary);
   return success;
}

bool psm_store_init(PsmStore* store, const char* state_path, const char* seed_path,
   char* error, size_t error_size) {
   if (!store || !state_path || !*state_path || !seed_path || !*seed_path)
      return fail(error, error_size, "PSM paths are required");
   memset(store, 0, sizeof(*store));
   if (pthread_mutex_init(&store->mutex, NULL) != 0) return fail(error, error_size, "cannot initialize PSM mutex");
   store->state_path = strdup(state_path);
   if (!store->state_path || !load_records(&store->records, state_path, true, false, error, error_size) ||
      !load_records(&store->records, seed_path, false, true, error, error_size)) {
      psm_store_destroy(store);
      return false;
   }
   return true;
}

void psm_store_destroy(PsmStore* store) {
   if (!store) return;
   free_records(store->records);
   store->records = NULL;
   free(store->state_path);
   store->state_path = NULL;
   pthread_mutex_destroy(&store->mutex);
}

rbusError_t psm_store_set(PsmStore* store, const char* name, rbusValue_t value) {
   const char* known_type = value ? type_name(rbusValue_GetType(value)) : NULL;
   if (!store || !name || !*name || !known_type) return RBUS_ERROR_INVALID_INPUT;
   char* string_value = rbusValue_ToString(value, NULL, 0);
   PsmRecord replacement = {.name = strdup(name), .type = rbusValue_GetType(value), .value = string_value};
   if (!replacement.name || !replacement.value) {
      free(replacement.name);
      free(replacement.value);
      return RBUS_ERROR_OUT_OF_RESOURCES;
   }

   pthread_mutex_lock(&store->mutex);
   PsmRecord* existing = find_record(store->records, name);
   PsmRecord* committed = existing ? NULL : malloc(sizeof(*committed));
   if (!existing && !committed) {
      pthread_mutex_unlock(&store->mutex);
      free(replacement.name);
      free(replacement.value);
      return RBUS_ERROR_OUT_OF_RESOURCES;
   }
   char* serialized = serialize_records(store->records, &replacement);
   if (!serialized || !atomic_write(store->state_path, serialized)) {
      free(serialized);
      free(committed);
      pthread_mutex_unlock(&store->mutex);
      free(replacement.name);
      free(replacement.value);
      return RBUS_ERROR_BUS_ERROR;
   }
   free(serialized);
   if (existing) {
      free(existing->value);
      existing->value = replacement.value;
      existing->type = replacement.type;
      free(replacement.name);
   } else {
      *committed = replacement;
      committed->next = store->records;
      store->records = committed;
   }
   pthread_mutex_unlock(&store->mutex);
   return RBUS_ERROR_SUCCESS;
}

rbusError_t psm_store_get(PsmStore* store, const char* name, rbusValue_t value) {
   if (!store || !name || !*name || !value) return RBUS_ERROR_INVALID_INPUT;
   pthread_mutex_lock(&store->mutex);
   PsmRecord* record = find_record(store->records, name);
   bool copied = record && rbusValue_SetFromString(value, record->type, record->value);
   pthread_mutex_unlock(&store->mutex);
   return !record ? RBUS_ERROR_ELEMENT_DOES_NOT_EXIST : copied ? RBUS_ERROR_SUCCESS : RBUS_ERROR_BUS_ERROR;
}