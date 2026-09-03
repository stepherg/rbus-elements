#include "rbus_elements.h"
#include "provider_state.h"

extern int g_totalElements;
extern DataElement* g_internalDataElements;
extern int g_num_tables;
extern TableDef* g_tables;
extern rbusHandle_t g_rbusHandle;

static bool parse_instance(const char* start, size_t length, uint32_t* instance) {
   if (length == 0) return false;
   uint32_t parsed = 0;
   for (size_t i = 0; i < length; i++) {
      if (start[i] < '0' || start[i] > '9') return false;
      uint32_t digit = (uint32_t)(start[i] - '0');
      if (parsed > (UINT32_MAX - digit) / 10) return false;
      parsed = parsed * 10 + digit;
   }
   if (parsed == 0) return false;
   *instance = parsed;
   return true;
}

char* get_table_name(const char* name, uint32_t* instance, char** property_name) {
   if (!name || !instance || !property_name) return NULL;
   *property_name = NULL;

   const char* selected_start = NULL;
   const char* selected_end = NULL;
   uint32_t selected_instance = 0;
   const char* segment_start = name;
   while (*segment_start) {
      const char* segment_end = strchr(segment_start, '.');
      if (!segment_end) segment_end = name + strlen(name);
      uint32_t candidate;
      if (segment_start != name && *segment_end == '.' && segment_end[1] != '\0' &&
         parse_instance(segment_start, (size_t)(segment_end - segment_start), &candidate)) {
         selected_start = segment_start;
         selected_end = segment_end;
         selected_instance = candidate;
      }
      if (*segment_end == '\0') break;
      segment_start = segment_end + 1;
   }

   if (!selected_start) return NULL;
   size_t table_length = (size_t)(selected_start - name);
   char* table = malloc(table_length + 1);
   if (!table) return NULL;
   memcpy(table, name, table_length);
   table[table_length] = '\0';

   *property_name = strdup(selected_end + 1);
   if (!*property_name) {
      free(table);
      return NULL;
   }
   *instance = selected_instance;
   return table;
}

rbusError_t getTableHandler(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
   (void)handle; (void)options;
   const char* name = rbusProperty_GetName(property);

   size_t name_length = name ? strlen(name) : 0;
   size_t suffix_length = strlen(TABLE_COUNT_PROP);
   if (name_length <= suffix_length || name_length >= MAX_NAME_LEN ||
      strcmp(name + name_length - suffix_length, TABLE_COUNT_PROP) != 0)
      return RBUS_ERROR_INVALID_INPUT;
   char table_name[MAX_NAME_LEN];
   memcpy(table_name, name, name_length - suffix_length);
   table_name[name_length - suffix_length] = '.';
   table_name[name_length - suffix_length + 1] = '\0';
   uint32_t count;
   if (!provider_table_count(table_name, &count)) return RBUS_ERROR_INVALID_INPUT;

   rbusValue_t value;
   rbusValue_Init(&value);
   rbusValue_SetUInt32(value, count);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

rbusError_t table_add_row(rbusHandle_t handle, const char* tableName, const char* aliasName, uint32_t* instNum) {
   (void)handle;
   return provider_table_add(tableName, aliasName, instNum);
}

rbusError_t table_remove_row(rbusHandle_t handle, const char* rowName) {
   if (!rowName) {
      return RBUS_ERROR_INVALID_INPUT;
   }

   size_t len = strlen(rowName);
   if (len == 0 || rowName[len - 1] != '.') {
      return RBUS_ERROR_INVALID_INPUT;
   }

   char* buf = strdup(rowName);
   if (!buf) {
      return RBUS_ERROR_OUT_OF_RESOURCES;
   }

   buf[len - 1] = '\0';  // Remove trailing dot

   char* last_dot = strrchr(buf, '.');
   if (!last_dot) {
      free(buf);
      return RBUS_ERROR_INVALID_INPUT;
   }

   char* inst_or_alias = last_dot + 1;
   *last_dot = '\0';  // buf now holds the prefix before the instance or alias

   char tableName[MAX_NAME_LEN];
   snprintf(tableName, MAX_NAME_LEN, "%s.", buf);  // Reconstruct table name with trailing dot

   uint32_t instance = 0;
   char* extracted_alias = NULL;
   bool is_numeric_inst = false;

   char* endptr;
   long inst_val = strtol(inst_or_alias, &endptr, 10);
   if (*endptr == '\0' && inst_val > 0 && inst_val <= INT32_MAX) {
      instance = (int)inst_val;
      is_numeric_inst = true;
   } else if (inst_or_alias[0] == '[' && inst_or_alias[strlen(inst_or_alias) - 1] == ']') {
      extracted_alias = strdup(inst_or_alias + 1);
      if (!extracted_alias) {
         free(buf);
         return RBUS_ERROR_OUT_OF_RESOURCES;
      }
      extracted_alias[strlen(extracted_alias) - 1] = '\0';  // Remove closing bracket
   } else {
      free(buf);
      return RBUS_ERROR_INVALID_INPUT;
   }

   free(buf);

   TableRow removed = {0};
   rbusError_t state_error = provider_table_remove(tableName,
      is_numeric_inst ? instance : 0, extracted_alias, &removed);
   free(extracted_alias);
   if (state_error != RBUS_ERROR_SUCCESS) return state_error;

   // Publish deletion event
   rbusEvent_t event = {.name = rowName, .type = RBUS_EVENT_OBJECT_DELETED, .data = NULL};
   rbusError_t rc = rbusEvent_Publish(handle, &event);
   if (rc != RBUS_ERROR_SUCCESS && rc != RBUS_ERROR_NOSUBSCRIBERS) {
      fprintf(stderr, "Failed to publish table remove event for %s: %d\n", rowName, rc);
   }

   provider_row_free(&removed);

   return RBUS_ERROR_SUCCESS;
}

void valueChangeHandler(rbusHandle_t handle, rbusEvent_t const* event, rbusEventSubscription_t* subscription) {
   (void)handle; (void)subscription;
   rbusValue_t newValue = rbusObject_GetValue(event->data, "value");
   if (!newValue) {
      fprintf(stderr, "Value change event for %s: No new value provided\n", event->name);
      return;
   }

   switch (rbusValue_GetType(newValue)) {
      case RBUS_STRING: {
         char* str = rbusValue_ToString(newValue, NULL, 0);
         fprintf(stderr, "Value changed for %s: %s\n", event->name, str);
         free(str);
         break;
      }
      case RBUS_INT32:
         fprintf(stderr, "Value changed for %s: %d\n", event->name, rbusValue_GetInt32(newValue));
         break;
      case RBUS_UINT32:
         fprintf(stderr, "Value changed for %s: %u\n", event->name, rbusValue_GetUInt32(newValue));
         break;
      case RBUS_BOOLEAN:
         fprintf(stderr, "Value changed for %s: %s\n", event->name, rbusValue_GetBoolean(newValue) ? "true" : "false");
         break;
      case RBUS_INT64:
         fprintf(stderr, "Value changed for %s: %lld\n", event->name, (long long)rbusValue_GetInt64(newValue));
         break;
      case RBUS_UINT64:
         fprintf(stderr, "Value changed for %s: %llu\n", event->name, (unsigned long long)rbusValue_GetUInt64(newValue));
         break;
      case RBUS_SINGLE:
         fprintf(stderr, "Value changed for %s: %f\n", event->name, rbusValue_GetSingle(newValue));
         break;
      case RBUS_DOUBLE:
         fprintf(stderr, "Value changed for %s: %lf\n", event->name, rbusValue_GetDouble(newValue));
         break;
      case RBUS_BYTE:
         fprintf(stderr, "Value changed for %s: %u\n", event->name, rbusValue_GetByte(newValue));
         break;
      default:
         fprintf(stderr, "Value changed for %s: Unsupported type\n", event->name);
         break;
   }
}

rbusError_t eventSubHandler(rbusHandle_t handle, rbusEventSubAction_t action, const char* eventName, rbusFilter_t filter, int32_t interval, bool* autoPublish) {
   (void)handle; (void)filter; (void)interval;
   fprintf(stderr, "Event subscription handler called for %s, action: %s\n", eventName,
      action == RBUS_EVENT_ACTION_SUBSCRIBE ? "subscribe" : "unsubscribe");

   *autoPublish = true;

   return RBUS_ERROR_SUCCESS;
}

rbusError_t getHandler(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
   (void)handle; (void)options;
   const char* name = rbusProperty_GetName(property);
   uint32_t inst;
   char* prop;
   char* tbl = get_table_name(name, &inst, &prop);
   if (tbl == NULL) {
      // Normal property
      DataElement* de = lookup_element(name);
      if(!de || de->elementType != RBUS_ELEMENT_TYPE_PROPERTY)
         return RBUS_ERROR_INVALID_INPUT;
      ElementValue snapshot;
      if (!provider_property_snapshot(de, &snapshot)) return RBUS_ERROR_OUT_OF_RESOURCES;
      rbusValue_t value;
      rbusValue_Init(&value);
      provider_value_to_rbus(de->type, &snapshot, value);
      rbusProperty_SetValue(property, value);
      rbusValue_Release(value);
      if (IS_STRING_TYPE(de->type)) free(snapshot.strVal);
      return RBUS_ERROR_SUCCESS;
   } else {
      // Row property
      char* wildcard = create_wildcard(name);
      DataElement* de = wildcard ? lookup_element(wildcard) : NULL;
      free(wildcard);
      if (!de || de->elementType != RBUS_ELEMENT_TYPE_PROPERTY) {
         free(tbl);
         free(prop);
         return RBUS_ERROR_BUS_ERROR;
      }
      ElementValue snapshot;
      rbusError_t state_error = provider_row_snapshot(tbl, inst, prop, de->type, &snapshot);
      free(tbl);
      free(prop);
      if (state_error != RBUS_ERROR_SUCCESS) {
         return state_error;
      }

      rbusValue_t value;
      rbusValue_Init(&value);
      provider_value_to_rbus(de->type, &snapshot, value);
      rbusProperty_SetValue(property, value);
      rbusValue_Release(value);
      if (IS_STRING_TYPE(de->type)) free(snapshot.strVal);
      return RBUS_ERROR_SUCCESS;
   }
}

rbusError_t setHandler(rbusHandle_t handle, rbusProperty_t property, rbusSetHandlerOptions_t* options) {
   (void)handle; (void)options;
   const char* name = rbusProperty_GetName(property);

   rbusValue_t value = rbusProperty_GetValue(property);
   uint32_t inst;
   char* prop;
   char* tbl = get_table_name(name, &inst, &prop);
   if (tbl == NULL) {
      // Normal property
      DataElement* de = lookup_element(name);
      if(!de || de->elementType != RBUS_ELEMENT_TYPE_PROPERTY)
         return RBUS_ERROR_INVALID_INPUT;
      ValueType type = de->type;
      if (!provider_value_type_matches(type, value)) return RBUS_ERROR_INVALID_INPUT;
      ElementValue replacement;
      if (!provider_value_from_rbus(type, value, &replacement)) return RBUS_ERROR_OUT_OF_RESOURCES;
      provider_property_replace(de, &replacement);
      return RBUS_ERROR_SUCCESS;
   } else {
      // Row property
      char* wildcard = create_wildcard(name);
      DataElement* de = wildcard ? lookup_element(wildcard) : NULL;
      free(wildcard);
      if (!de || de->elementType != RBUS_ELEMENT_TYPE_PROPERTY) {
         free(tbl);
         free(prop);
         return RBUS_ERROR_BUS_ERROR;
      }
      rbusError_t state_error = provider_row_set(tbl, inst, prop, de->type, value);
      free(tbl);
      free(prop);
      return state_error;
   }
}
