#include "rbus_elements.h"

extern int g_totalElements;
extern DataElement* g_internalDataElements;
extern int g_num_tables;
extern TableDef* g_tables;
extern rbusHandle_t g_rbusHandle;

char* get_table_name(const char* name, uint32_t* instance, char** property_name) {
   char* dup = strdup(name);
   if (!dup) return NULL;
   int num_segments = 0;
   char* temp = dup;
   while (*temp) {
      if (*temp++ == '.') num_segments++;
   }
   num_segments++; // number of segments = dots +1
   char** segments = malloc(num_segments * sizeof(char*));
   if (!segments) {
      free(dup);
      return NULL;
   }
   temp = dup;
   int i = 0;
   char* token;
   while ((token = strsep(&temp, ".")) != NULL) {
      segments[i++] = token;
   }
   // now find rightmost i where segments[i] is number
   int inst_index = -1;
   for (int j = num_segments - 1; j >= 0; j--) {
      char* endptr;
      errno = 0;
      unsigned long val = strtoul(segments[j], &endptr, 10);
      if (errno == 0 && *endptr == '\0' && val > 0 && val <= UINT32_MAX && segments[j][0] != '\0') {
         inst_index = j;
         *instance = (uint32_t)val;
         break;
      }
   }
   if (inst_index == -1 || inst_index == num_segments - 1 || inst_index == 0) { // no instance, or instance is last (no property), or first (invalid)
      free(segments);
      free(dup);
      return NULL;
   }
   // now build table: segments 0 to inst_index-1 + '.'
   size_t table_len = 0;
   for (int j = 0; j < inst_index; j++) {
      table_len += strlen(segments[j]) + 1;
   }
   char* table = malloc(table_len + 1); // +1 for null
   if (!table) {
      free(segments);
      free(dup);
      return NULL;
   }
   char* p = table;
   for (int j = 0; j < inst_index; j++) {
      strcpy(p, segments[j]);
      p += strlen(segments[j]);
      *p++ = '.';
   }
   *p = '\0';
   // now property: segments inst_index+1 to end, with .
   size_t prop_len = 0;
   for (int j = inst_index + 1; j < num_segments; j++) {
      prop_len += strlen(segments[j]) + (j < num_segments - 1 ? 1 : 0);
   }
   *property_name = malloc(prop_len + 1);
   if (!*property_name) {
      free(table);
      free(segments);
      free(dup);
      return NULL;
   }
   p = *property_name;
   for (int j = inst_index + 1; j < num_segments; j++) {
      strcpy(p, segments[j]);
      p += strlen(segments[j]);
      if (j < num_segments - 1) {
         *p++ = '.';
      }
   }
   *p = '\0';
   free(segments);
   free(dup);
   return table;
}

rbusError_t getTableHandler(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
   const char* name = rbusProperty_GetName(property);

   char table_name[MAX_NAME_LEN];
   strncpy(table_name, name, MAX_NAME_LEN);
   int slen = strlen(table_name);
   table_name[slen - strlen(TABLE_COUNT_PROP)] = '.';
   table_name[slen - strlen(TABLE_COUNT_PROP) + 1] = '\0';
   TableDef* table = NULL;
   for (int i = 0; i < g_num_tables; i++) {
      if (strcmp(g_tables[i].name, table_name) == 0) {
         table = &g_tables[i];
         break;
      }
   }
   if (!table) {
      return RBUS_ERROR_INVALID_INPUT;
   }

   rbusValue_t value;
   rbusValue_Init(&value);
   rbusValue_SetUInt32(value, table->num_inst);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

rbusError_t table_add_row(rbusHandle_t handle, const char* tableName, const char* aliasName, uint32_t* instNum) {
   if (!tableName || !instNum) {
      return RBUS_ERROR_INVALID_INPUT;
   }

   // Find or create TableDef
   TableDef* table = NULL;
   for (int i = 0; i < g_num_tables; i++) {
      if (strcmp(g_tables[i].name, tableName) == 0) {
         table = &g_tables[i];
         break;
      }
   }
   if (!table) {
      g_tables = realloc(g_tables, (g_num_tables + 1) * sizeof(TableDef));
      table = &g_tables[g_num_tables++];
      strcpy(table->name, tableName);
      table->rows = NULL;
      table->num_rows = 0;
      table->next_inst = 1;
   }

   // Check for duplicate alias if provided
   if (aliasName && aliasName[0] != '\0') {
      for (int j = 0; j < table->num_rows; j++) {
         if (strcmp(table->rows[j].alias, aliasName) == 0) {
            return RBUS_ERROR_ELEMENT_NAME_DUPLICATE;
         }
      }
   }

   table->rows = realloc(table->rows, (table->num_rows + 1) * sizeof(TableRow));
   TableRow* row = &table->rows[table->num_rows];
   snprintf(row->name, MAX_NAME_LEN, "%s%u.", tableName, table->next_inst);
   row->instNum = table->next_inst++;
   table->num_inst++;
   strncpy(row->alias, aliasName ? aliasName : "", MAX_NAME_LEN - 1);
   row->alias[MAX_NAME_LEN - 1] = '\0';
   row->props = NULL;
   *instNum = row->instNum;
   table->num_rows++;

   // fprintf(stderr, "table_add_row: %s, instNum: %d\n", row->name, *instNum);

   return RBUS_ERROR_SUCCESS;
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

   int instance = 0;
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

   // Find the table
   TableDef* table = NULL;
   for (int i = 0; i < g_num_tables; i++) {
      if (strcmp(g_tables[i].name, tableName) == 0) {
         table = &g_tables[i];
         break;
      }
   }
   if (!table) {
      free(extracted_alias);
      return RBUS_ERROR_INVALID_INPUT;
   }

   // Find the row index
   int row_index = -1;
   if (is_numeric_inst) {
      for (int i = 0; i < table->num_rows; i++) {
         if (table->rows[i].instNum == instance) {
            row_index = i;
            break;
         }
      }
   } else {
      for (int i = 0; i < table->num_rows; i++) {
         if (table->rows[i].alias[0] && strcmp(table->rows[i].alias, extracted_alias) == 0) {
            row_index = i;
            break;
         }
      }
      free(extracted_alias);
   }

   if (row_index == -1) {
      return RBUS_ERROR_INVALID_INPUT;
   }

   // Free row properties
   RowProperty* p = table->rows[row_index].props;
   while (p) {
      RowProperty* next = p->next;
      if (IS_STRING_TYPE(p->type)) {
         free(p->value.strVal);
      }
      free(p);
      p = next;
   }

   // Remove the row
   memmove(&table->rows[row_index], &table->rows[row_index + 1], (table->num_rows - row_index - 1) * sizeof(TableRow));
   table->num_rows--;
   table->rows = realloc(table->rows, table->num_rows * sizeof(TableRow));

   // Publish deletion event
   rbusEvent_t event = {.name = rowName, .type = RBUS_EVENT_OBJECT_DELETED, .data = NULL};
   rbusError_t rc = rbusEvent_Publish(handle, &event);
   if (rc != RBUS_ERROR_SUCCESS && rc != RBUS_ERROR_NOSUBSCRIBERS) {
      fprintf(stderr, "Failed to publish table remove event for %s: %d\n", rowName, rc);
   }

   if (table->num_inst > 0) {
      table->num_inst--;
   }

   return RBUS_ERROR_SUCCESS;
}

void valueChangeHandler(rbusHandle_t handle, rbusEvent_t const* event, rbusEventSubscription_t* subscription) {
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
   fprintf(stderr, "Event subscription handler called for %s, action: %s\n", eventName,
      action == RBUS_EVENT_ACTION_SUBSCRIBE ? "subscribe" : "unsubscribe");

   *autoPublish = true;

   return RBUS_ERROR_SUCCESS;
}

rbusError_t getHandler(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
   const char* name = rbusProperty_GetName(property);
   uint32_t inst;
   char* prop;
   char* tbl = get_table_name(name, &inst, &prop);
   if (tbl == NULL) {
      // Normal property
      for (int i = 0; i < g_totalElements; i++) {
         if (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY && strcmp(name, g_internalDataElements[i].name) == 0) {
            rbusValue_t value;
            rbusValue_Init(&value);
            switch (g_internalDataElements[i].type) {
               case TYPE_STRING:
               case TYPE_DATETIME:
               case TYPE_BASE64:
                  rbusValue_SetString(value, g_internalDataElements[i].value.strVal);
                  break;
               case TYPE_INT:
                  rbusValue_SetInt32(value, g_internalDataElements[i].value.intVal);
                  break;
               case TYPE_UINT:
                  rbusValue_SetUInt32(value, g_internalDataElements[i].value.uintVal);
                  break;
               case TYPE_BOOL:
                  rbusValue_SetBoolean(value, g_internalDataElements[i].value.boolVal);
                  break;
               case TYPE_LONG:
                  rbusValue_SetInt64(value, g_internalDataElements[i].value.longVal);
                  break;
               case TYPE_ULONG:
                  rbusValue_SetUInt64(value, g_internalDataElements[i].value.ulongVal);
                  break;
               case TYPE_FLOAT:
                  rbusValue_SetSingle(value, g_internalDataElements[i].value.floatVal);
                  break;
               case TYPE_DOUBLE:
                  rbusValue_SetDouble(value, g_internalDataElements[i].value.doubleVal);
                  break;
               case TYPE_BYTE:
                  rbusValue_SetByte(value, g_internalDataElements[i].value.byteVal);
                  break;
            }
            rbusProperty_SetValue(property, value);
            rbusValue_Release(value);
            return RBUS_ERROR_SUCCESS;
         }
      }
      return RBUS_ERROR_INVALID_INPUT;
   } else {
      // Row property
      TableDef* table = NULL;
      for (int i = 0; i < g_num_tables; i++) {
         if (strcmp(g_tables[i].name, tbl) == 0) {
            table = &g_tables[i];
            break;
         }
      }
      if (!table) {
         free(tbl);
         free(prop);
         return RBUS_ERROR_BUS_ERROR;
      }

      TableRow* row = NULL;
      for (int i = 0; i < table->num_rows; i++) {
         if (table->rows[i].instNum == inst) {
            row = &table->rows[i];
            break;
         }
      }
      if (!row) {
         free(tbl);
         free(prop);
         return RBUS_ERROR_BUS_ERROR;
      }

      RowProperty* p = row->props;
      while (p) {
         if (strcmp(p->name, prop) == 0) {
            break;
         }
         p = p->next;
      }

      if (!p) {
         // Add with default
         char wildcard[MAX_NAME_LEN];
         snprintf(wildcard, MAX_NAME_LEN, "%s{i}.%s", tbl, prop);
         DataElement* de = NULL;
         for (int i = 0; i < g_totalElements; i++) {
            if (strcmp(g_internalDataElements[i].name, wildcard) == 0 && g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY) {
               de = &g_internalDataElements[i];
               break;
            }
         }
         if (!de) {
            free(tbl);
            free(prop);
            return RBUS_ERROR_BUS_ERROR;
         }

         p = (RowProperty*)malloc(sizeof(RowProperty));
         if (!p) {
            free(tbl);
            free(prop);
            return RBUS_ERROR_BUS_ERROR;
         }

         strcpy(p->name, prop);
         p->type = de->type;
         switch (p->type) {
            case TYPE_STRING:
            case TYPE_DATETIME:
            case TYPE_BASE64:
               p->value.strVal = strdup("");
               break;
            case TYPE_BOOL:
               p->value.boolVal = false;
               break;
            default:
               memset(&p->value, 0, sizeof(p->value));
               break;
         }
         p->next = row->props;
         row->props = p;
      }

      rbusValue_t value;
      rbusValue_Init(&value);
      switch (p->type) {
         case TYPE_STRING:
         case TYPE_DATETIME:
         case TYPE_BASE64:
            rbusValue_SetString(value, p->value.strVal);
            break;
         case TYPE_INT:
            rbusValue_SetInt32(value, p->value.intVal);
            break;
         case TYPE_UINT:
            rbusValue_SetUInt32(value, p->value.uintVal);
            break;
         case TYPE_BOOL:
            rbusValue_SetBoolean(value, p->value.boolVal);
            break;
         case TYPE_LONG:
            rbusValue_SetInt64(value, p->value.longVal);
            break;
         case TYPE_ULONG:
            rbusValue_SetUInt64(value, p->value.ulongVal);
            break;
         case TYPE_FLOAT:
            rbusValue_SetSingle(value, p->value.floatVal);
            break;
         case TYPE_DOUBLE:
            rbusValue_SetDouble(value, p->value.doubleVal);
            break;
         case TYPE_BYTE:
            rbusValue_SetByte(value, p->value.byteVal);
            break;
      }
      rbusProperty_SetValue(property, value);
      rbusValue_Release(value);
      free(tbl);
      free(prop);
      return RBUS_ERROR_SUCCESS;
   }
}

rbusError_t setHandler(rbusHandle_t handle, rbusProperty_t property, rbusSetHandlerOptions_t* options) {
   const char* name = rbusProperty_GetName(property);

   rbusValue_t value = rbusProperty_GetValue(property);
   uint32_t inst;
   char* prop;
   char* tbl = get_table_name(name, &inst, &prop);
   if (tbl == NULL) {
      // Normal property
      for (int i = 0; i < g_totalElements; i++) {
         if (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY && strcmp(name, g_internalDataElements[i].name) == 0) {
            ValueType type = g_internalDataElements[i].type;
            rbusValueType_t vt = rbusValue_GetType(value);
            // Check type match
            if ((type == TYPE_STRING && vt != RBUS_STRING) ||
               (type == TYPE_INT && vt != RBUS_INT32) ||
               (type == TYPE_UINT && vt != RBUS_UINT32) ||
               (type == TYPE_BOOL && vt != RBUS_BOOLEAN) ||
               (type == TYPE_DATETIME && vt != RBUS_STRING) ||
               (type == TYPE_BASE64 && vt != RBUS_STRING) ||
               (type == TYPE_LONG && vt != RBUS_INT64) ||
               (type == TYPE_ULONG && vt != RBUS_UINT64) ||
               (type == TYPE_FLOAT && vt != RBUS_SINGLE) ||
               (type == TYPE_DOUBLE && vt != RBUS_DOUBLE) ||
               (type == TYPE_BYTE && vt != RBUS_BYTE)) {
               return RBUS_ERROR_INVALID_INPUT;
            }

            if (IS_STRING_TYPE(type)) {
               free(g_internalDataElements[i].value.strVal);
               g_internalDataElements[i].value.strVal = strdup(rbusValue_GetString(value, NULL));
               if (!g_internalDataElements[i].value.strVal) {
                  return RBUS_ERROR_OUT_OF_RESOURCES;
               }
            } else {
               switch (type) {
                  case TYPE_INT:
                     g_internalDataElements[i].value.intVal = rbusValue_GetInt32(value);
                     break;
                  case TYPE_UINT:
                     g_internalDataElements[i].value.uintVal = rbusValue_GetUInt32(value);
                     break;
                  case TYPE_BOOL:
                     g_internalDataElements[i].value.boolVal = rbusValue_GetBoolean(value);
                     break;
                  case TYPE_LONG:
                     g_internalDataElements[i].value.longVal = rbusValue_GetInt64(value);
                     break;
                  case TYPE_ULONG:
                     g_internalDataElements[i].value.ulongVal = rbusValue_GetUInt64(value);
                     break;
                  case TYPE_FLOAT:
                     g_internalDataElements[i].value.floatVal = rbusValue_GetSingle(value);
                     break;
                  case TYPE_DOUBLE:
                     g_internalDataElements[i].value.doubleVal = rbusValue_GetDouble(value);
                     break;
                  case TYPE_BYTE:
                     g_internalDataElements[i].value.byteVal = rbusValue_GetByte(value);
                     break;
                  default:
                     break;
               }
            }
            return RBUS_ERROR_SUCCESS;
         }
      }
      return RBUS_ERROR_INVALID_INPUT;
   } else {
      // Row property
      TableDef* table = NULL;
      for (int i = 0; i < g_num_tables; i++) {
         if (strcmp(g_tables[i].name, tbl) == 0) {
            table = &g_tables[i];
            break;
         }
      }
      if (!table) {
         free(tbl);
         free(prop);
         return RBUS_ERROR_BUS_ERROR;
      }

      TableRow* row = NULL;
      for (int i = 0; i < table->num_rows; i++) {
         if (table->rows[i].instNum == inst) {
            row = &table->rows[i];
            break;
         }
      }
      if (!row) {
         free(tbl);
         free(prop);
         return RBUS_ERROR_BUS_ERROR;
      }

      RowProperty* p = row->props;
      RowProperty* prev = NULL;
      while (p) {
         if (strcmp(p->name, prop) == 0) {
            break;
         }
         prev = p;
         p = p->next;
      }

      ValueType type;
      if (!p) {
         char* wildcard = create_wildcard(name);
         DataElement* de = NULL;
         for (int i = 0; i < g_totalElements; i++) {
            if (strcmp(g_internalDataElements[i].name, wildcard) == 0 && g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY) {
               de = &g_internalDataElements[i];
               break;
            }
         }
         if (!de) {
            free(tbl);
            free(prop);
            free(wildcard);
            return RBUS_ERROR_BUS_ERROR;
         }

         p = (RowProperty*)malloc(sizeof(RowProperty));
         if (!p) {
            free(tbl);
            free(prop);
            free(wildcard);
            return RBUS_ERROR_BUS_ERROR;
         }

         strcpy(p->name, prop);
         p->type = de->type;
         memset(&p->value, 0, sizeof(p->value));
         p->next = NULL;

         if (prev) {
            prev->next = p;
         } else {
            row->props = p;
         }
         free(wildcard);
      }

      type = p->type;
      rbusValueType_t vt = rbusValue_GetType(value);
      if ((type == TYPE_STRING && vt != RBUS_STRING) ||
         (type == TYPE_INT && vt != RBUS_INT32) ||
         (type == TYPE_UINT && vt != RBUS_UINT32) ||
         (type == TYPE_BOOL && vt != RBUS_BOOLEAN) ||
         (type == TYPE_DATETIME && vt != RBUS_STRING) ||
         (type == TYPE_BASE64 && vt != RBUS_STRING) ||
         (type == TYPE_LONG && vt != RBUS_INT64) ||
         (type == TYPE_ULONG && vt != RBUS_UINT64) ||
         (type == TYPE_FLOAT && vt != RBUS_SINGLE) ||
         (type == TYPE_DOUBLE && vt != RBUS_DOUBLE) ||
         (type == TYPE_BYTE && vt != RBUS_BYTE)) {
         free(tbl);
         free(prop);
         return RBUS_ERROR_INVALID_INPUT;
      }

      if (IS_STRING_TYPE(type)) {
         free(p->value.strVal);
         p->value.strVal = strdup(rbusValue_GetString(value, NULL));
         if (!p->value.strVal) {
            free(tbl);
            free(prop);
            return RBUS_ERROR_OUT_OF_RESOURCES;
         }
      } else {
         switch (type) {
            case TYPE_INT:
               p->value.intVal = rbusValue_GetInt32(value);
               break;
            case TYPE_UINT:
               p->value.uintVal = rbusValue_GetUInt32(value);
               break;
            case TYPE_BOOL:
               p->value.boolVal = rbusValue_GetBoolean(value);
               break;
            case TYPE_LONG:
               p->value.longVal = rbusValue_GetInt64(value);
               break;
            case TYPE_ULONG:
               p->value.ulongVal = rbusValue_GetUInt64(value);
               break;
            case TYPE_FLOAT:
               p->value.floatVal = rbusValue_GetSingle(value);
               break;
            case TYPE_DOUBLE:
               p->value.doubleVal = rbusValue_GetDouble(value);
               break;
            case TYPE_BYTE:
               p->value.byteVal = rbusValue_GetByte(value);
               break;
            default:
               break;
         }
      }
      free(tbl);
      free(prop);
      return RBUS_ERROR_SUCCESS;
   }
}
