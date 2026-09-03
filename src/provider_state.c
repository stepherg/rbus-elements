#include "provider_state.h"

pthread_mutex_t g_provider_mutex = PTHREAD_MUTEX_INITIALIZER;

extern TableDef* g_tables;
extern int g_num_tables;

static TableDef* find_table_locked(const char* name) {
   for (int i = 0; i < g_num_tables; i++) {
      if (strcmp(g_tables[i].name, name) == 0) return &g_tables[i];
   }
   return NULL;
}

static TableRow* find_row_locked(TableDef* table, uint32_t instance) {
   for (int i = 0; table && i < table->num_rows; i++) {
      if (table->rows[i].instNum == instance) return &table->rows[i];
   }
   return NULL;
}

static RowProperty* find_property_locked(TableRow* row, const char* name) {
   for (RowProperty* property = row ? row->props : NULL; property; property = property->next) {
      if (strcmp(property->name, name) == 0) return property;
   }
   return NULL;
}

bool provider_value_type_matches(ValueType type, rbusValue_t value) {
   if (!value) return false;
   rbusValueType_t actual = rbusValue_GetType(value);
   switch (type) {
      case TYPE_STRING:
      case TYPE_DATETIME:
      case TYPE_BASE64: return actual == RBUS_STRING;
      case TYPE_INT: return actual == RBUS_INT32;
      case TYPE_UINT: return actual == RBUS_UINT32;
      case TYPE_BOOL: return actual == RBUS_BOOLEAN;
      case TYPE_LONG: return actual == RBUS_INT64;
      case TYPE_ULONG: return actual == RBUS_UINT64;
      case TYPE_FLOAT: return actual == RBUS_SINGLE;
      case TYPE_DOUBLE: return actual == RBUS_DOUBLE;
      case TYPE_BYTE: return actual == RBUS_BYTE;
   }
   return false;
}

bool provider_value_from_rbus(ValueType type, rbusValue_t source, ElementValue* destination) {
   if (!destination || !provider_value_type_matches(type, source)) return false;
   memset(destination, 0, sizeof(*destination));
   switch (type) {
      case TYPE_STRING:
      case TYPE_DATETIME:
      case TYPE_BASE64:
         destination->strVal = strdup(rbusValue_GetString(source, NULL));
         return destination->strVal != NULL;
      case TYPE_INT: destination->intVal = rbusValue_GetInt32(source); break;
      case TYPE_UINT: destination->uintVal = rbusValue_GetUInt32(source); break;
      case TYPE_BOOL: destination->boolVal = rbusValue_GetBoolean(source); break;
      case TYPE_LONG: destination->longVal = rbusValue_GetInt64(source); break;
      case TYPE_ULONG: destination->ulongVal = rbusValue_GetUInt64(source); break;
      case TYPE_FLOAT: destination->floatVal = rbusValue_GetSingle(source); break;
      case TYPE_DOUBLE: destination->doubleVal = rbusValue_GetDouble(source); break;
      case TYPE_BYTE: destination->byteVal = rbusValue_GetByte(source); break;
   }
   return true;
}

void provider_value_to_rbus(ValueType type, const ElementValue* source, rbusValue_t destination) {
   switch (type) {
      case TYPE_STRING:
      case TYPE_DATETIME:
      case TYPE_BASE64: rbusValue_SetString(destination, source->strVal); break;
      case TYPE_INT: rbusValue_SetInt32(destination, source->intVal); break;
      case TYPE_UINT: rbusValue_SetUInt32(destination, source->uintVal); break;
      case TYPE_BOOL: rbusValue_SetBoolean(destination, source->boolVal); break;
      case TYPE_LONG: rbusValue_SetInt64(destination, source->longVal); break;
      case TYPE_ULONG: rbusValue_SetUInt64(destination, source->ulongVal); break;
      case TYPE_FLOAT: rbusValue_SetSingle(destination, source->floatVal); break;
      case TYPE_DOUBLE: rbusValue_SetDouble(destination, source->doubleVal); break;
      case TYPE_BYTE: rbusValue_SetByte(destination, source->byteVal); break;
   }
}

bool provider_property_snapshot(const DataElement* element, ElementValue* snapshot) {
   if (!element || !snapshot) return false;
   pthread_mutex_lock(&g_provider_mutex);
   if (IS_STRING_TYPE(element->type)) {
      snapshot->strVal = strdup(element->value.strVal);
      bool copied = snapshot->strVal != NULL;
      pthread_mutex_unlock(&g_provider_mutex);
      return copied;
   }
   *snapshot = element->value;
   pthread_mutex_unlock(&g_provider_mutex);
   return true;
}

void provider_property_replace(DataElement* element, ElementValue* replacement) {
   ElementValue previous;
   pthread_mutex_lock(&g_provider_mutex);
   previous = element->value;
   element->value = *replacement;
   memset(replacement, 0, sizeof(*replacement));
   pthread_mutex_unlock(&g_provider_mutex);
   if (IS_STRING_TYPE(element->type)) free(previous.strVal);
}

bool provider_table_count(const char* table_name, uint32_t* count) {
   if (!table_name || !count) return false;
   pthread_mutex_lock(&g_provider_mutex);
   TableDef* table = find_table_locked(table_name);
   if (table) *count = (uint32_t)table->num_rows;
   pthread_mutex_unlock(&g_provider_mutex);
   return table != NULL;
}

rbusError_t provider_table_add(const char* table_name, const char* alias, uint32_t* instance) {
   if (!table_name || !instance || strlen(table_name) >= MAX_NAME_LEN ||
      (alias && strlen(alias) >= MAX_NAME_LEN)) return RBUS_ERROR_INVALID_INPUT;
   const char* safe_alias = alias ? alias : "";

   pthread_mutex_lock(&g_provider_mutex);
   TableDef* table = find_table_locked(table_name);
   if (!table) {
      TableDef* resized = realloc(g_tables, (size_t)(g_num_tables + 1) * sizeof(*resized));
      if (!resized) {
         pthread_mutex_unlock(&g_provider_mutex);
         return RBUS_ERROR_OUT_OF_RESOURCES;
      }
      g_tables = resized;
      table = &g_tables[g_num_tables++];
      memset(table, 0, sizeof(*table));
      snprintf(table->name, sizeof(table->name), "%s", table_name);
      table->next_inst = 1;
   }
   for (int i = 0; safe_alias[0] && i < table->num_rows; i++) {
      if (strcmp(table->rows[i].alias, safe_alias) == 0) {
         pthread_mutex_unlock(&g_provider_mutex);
         return RBUS_ERROR_ELEMENT_NAME_DUPLICATE;
      }
   }
   if (table->next_inst == 0) {
      pthread_mutex_unlock(&g_provider_mutex);
      return RBUS_ERROR_OUT_OF_RESOURCES;
   }

   char row_name[MAX_NAME_LEN];
   int written = snprintf(row_name, sizeof(row_name), "%s%u.", table_name, table->next_inst);
   if (written < 0 || (size_t)written >= sizeof(row_name)) {
      pthread_mutex_unlock(&g_provider_mutex);
      return RBUS_ERROR_INVALID_INPUT;
   }
   TableRow* resized = realloc(table->rows, (size_t)(table->num_rows + 1) * sizeof(*resized));
   if (!resized) {
      pthread_mutex_unlock(&g_provider_mutex);
      return RBUS_ERROR_OUT_OF_RESOURCES;
   }
   table->rows = resized;
   TableRow* row = &table->rows[table->num_rows++];
   memset(row, 0, sizeof(*row));
   snprintf(row->name, sizeof(row->name), "%s", row_name);
   snprintf(row->alias, sizeof(row->alias), "%s", safe_alias);
   row->instNum = table->next_inst;
   table->num_inst = (uint32_t)table->num_rows;
   table->next_inst = table->next_inst == UINT32_MAX ? 0 : table->next_inst + 1;
   *instance = row->instNum;
   pthread_mutex_unlock(&g_provider_mutex);
   return RBUS_ERROR_SUCCESS;
}

rbusError_t provider_table_commit_registered(const char* table_name, uint32_t instance, const char* alias) {
   if (!table_name || !instance || strlen(table_name) >= MAX_NAME_LEN ||
      (alias && strlen(alias) >= MAX_NAME_LEN)) return RBUS_ERROR_INVALID_INPUT;
   const char* safe_alias = alias ? alias : "";
   char row_name[MAX_NAME_LEN];
   int written = snprintf(row_name, sizeof(row_name), "%s%u.", table_name, instance);
   if (written < 0 || (size_t)written >= sizeof(row_name)) return RBUS_ERROR_INVALID_INPUT;

   pthread_mutex_lock(&g_provider_mutex);
   TableDef* table = find_table_locked(table_name);
   if (!table) {
      TableDef* resized = realloc(g_tables, (size_t)(g_num_tables + 1) * sizeof(*resized));
      if (!resized) {
         pthread_mutex_unlock(&g_provider_mutex);
         return RBUS_ERROR_OUT_OF_RESOURCES;
      }
      g_tables = resized;
      table = &g_tables[g_num_tables++];
      memset(table, 0, sizeof(*table));
      snprintf(table->name, sizeof(table->name), "%s", table_name);
      table->next_inst = 1;
   }
   for (int i = 0; i < table->num_rows; i++) {
      if (table->rows[i].instNum == instance ||
         (safe_alias[0] && strcmp(table->rows[i].alias, safe_alias) == 0)) {
         pthread_mutex_unlock(&g_provider_mutex);
         return RBUS_ERROR_ELEMENT_NAME_DUPLICATE;
      }
   }
   TableRow* resized = realloc(table->rows, (size_t)(table->num_rows + 1) * sizeof(*resized));
   if (!resized) {
      pthread_mutex_unlock(&g_provider_mutex);
      return RBUS_ERROR_OUT_OF_RESOURCES;
   }
   table->rows = resized;
   TableRow* row = &table->rows[table->num_rows++];
   memset(row, 0, sizeof(*row));
   snprintf(row->name, sizeof(row->name), "%s", row_name);
   snprintf(row->alias, sizeof(row->alias), "%s", safe_alias);
   row->instNum = instance;
   table->num_inst = (uint32_t)table->num_rows;
   if (table->next_inst && instance >= table->next_inst)
      table->next_inst = instance == UINT32_MAX ? 0 : instance + 1;
   pthread_mutex_unlock(&g_provider_mutex);
   return RBUS_ERROR_SUCCESS;
}

rbusError_t provider_table_remove(const char* table_name, uint32_t instance,
   const char* alias, TableRow* removed) {
   if (!table_name || !removed) return RBUS_ERROR_INVALID_INPUT;
   pthread_mutex_lock(&g_provider_mutex);
   TableDef* table = find_table_locked(table_name);
   int row_index = -1;
   for (int i = 0; table && i < table->num_rows; i++) {
      if ((instance && table->rows[i].instNum == instance) ||
         (!instance && alias && table->rows[i].alias[0] && strcmp(table->rows[i].alias, alias) == 0)) {
         row_index = i;
         break;
      }
   }
   if (!table || row_index < 0) {
      pthread_mutex_unlock(&g_provider_mutex);
      return RBUS_ERROR_INVALID_INPUT;
   }
   *removed = table->rows[row_index];
   memmove(&table->rows[row_index], &table->rows[row_index + 1],
      (size_t)(table->num_rows - row_index - 1) * sizeof(*table->rows));
   table->num_rows--;
   table->num_inst = (uint32_t)table->num_rows;
   pthread_mutex_unlock(&g_provider_mutex);
   return RBUS_ERROR_SUCCESS;
}

rbusError_t provider_row_snapshot(const char* table_name, uint32_t instance,
   const char* property_name, ValueType type, ElementValue* snapshot) {
   if (!table_name || !property_name || !snapshot || strlen(property_name) >= MAX_NAME_LEN)
      return RBUS_ERROR_INVALID_INPUT;
   RowProperty* candidate = calloc(1, sizeof(*candidate));
   if (!candidate) return RBUS_ERROR_OUT_OF_RESOURCES;
   snprintf(candidate->name, sizeof(candidate->name), "%s", property_name);
   candidate->type = type;
   if (IS_STRING_TYPE(type)) {
      candidate->value.strVal = strdup("");
      if (!candidate->value.strVal) {
         free(candidate);
         return RBUS_ERROR_OUT_OF_RESOURCES;
      }
   }

   pthread_mutex_lock(&g_provider_mutex);
   TableDef* table = find_table_locked(table_name);
   TableRow* row = find_row_locked(table, instance);
   if (!row) {
      pthread_mutex_unlock(&g_provider_mutex);
      provider_row_free(&(TableRow){.props = candidate});
      return RBUS_ERROR_BUS_ERROR;
   }
   RowProperty* property = find_property_locked(row, property_name);
   if (!property) {
      candidate->next = row->props;
      row->props = candidate;
      property = candidate;
      candidate = NULL;
   }
   if (property->type != type) {
      pthread_mutex_unlock(&g_provider_mutex);
      provider_row_free(&(TableRow){.props = candidate});
      return RBUS_ERROR_INVALID_INPUT;
   }
   if (IS_STRING_TYPE(type)) {
      snapshot->strVal = strdup(property->value.strVal);
      if (!snapshot->strVal) {
         pthread_mutex_unlock(&g_provider_mutex);
         provider_row_free(&(TableRow){.props = candidate});
         return RBUS_ERROR_OUT_OF_RESOURCES;
      }
   } else {
      *snapshot = property->value;
   }
   pthread_mutex_unlock(&g_provider_mutex);
   provider_row_free(&(TableRow){.props = candidate});
   return RBUS_ERROR_SUCCESS;
}

rbusError_t provider_row_set(const char* table_name, uint32_t instance,
   const char* property_name, ValueType type, rbusValue_t value) {
   if (!table_name || !property_name || strlen(property_name) >= MAX_NAME_LEN ||
      !provider_value_type_matches(type, value)) return RBUS_ERROR_INVALID_INPUT;
   ElementValue replacement;
   if (!provider_value_from_rbus(type, value, &replacement)) return RBUS_ERROR_OUT_OF_RESOURCES;
   RowProperty* candidate = calloc(1, sizeof(*candidate));
   if (!candidate) {
      if (IS_STRING_TYPE(type)) free(replacement.strVal);
      return RBUS_ERROR_OUT_OF_RESOURCES;
   }
   snprintf(candidate->name, sizeof(candidate->name), "%s", property_name);
   candidate->type = type;
   candidate->value = replacement;

   ElementValue previous = {0};
   pthread_mutex_lock(&g_provider_mutex);
   TableDef* table = find_table_locked(table_name);
   TableRow* row = find_row_locked(table, instance);
   if (!row) {
      pthread_mutex_unlock(&g_provider_mutex);
      provider_row_free(&(TableRow){.props = candidate});
      return RBUS_ERROR_BUS_ERROR;
   }
   RowProperty* property = find_property_locked(row, property_name);
   if (property && property->type != type) {
      pthread_mutex_unlock(&g_provider_mutex);
      provider_row_free(&(TableRow){.props = candidate});
      return RBUS_ERROR_INVALID_INPUT;
   }
   if (property) {
      previous = property->value;
      property->value = candidate->value;
      free(candidate);
   } else {
      candidate->next = row->props;
      row->props = candidate;
   }
   pthread_mutex_unlock(&g_provider_mutex);
   if (IS_STRING_TYPE(type)) free(previous.strVal);
   return RBUS_ERROR_SUCCESS;
}

bool provider_row_property_exists(const char* table_name, uint32_t instance,
   const char* property_name) {
   pthread_mutex_lock(&g_provider_mutex);
   bool exists = find_property_locked(find_row_locked(find_table_locked(table_name), instance), property_name) != NULL;
   pthread_mutex_unlock(&g_provider_mutex);
   return exists;
}

void provider_row_free(TableRow* row) {
   if (!row) return;
   RowProperty* property = row->props;
   while (property) {
      RowProperty* next = property->next;
      if (IS_STRING_TYPE(property->type)) free(property->value.strVal);
      free(property);
      property = next;
   }
   row->props = NULL;
}