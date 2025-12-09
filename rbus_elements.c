#include "rbus_elements.h"

DataElement* g_internalDataElements = NULL;
static int g_numElements = 0;
int g_totalElements = 0;
rbusHandle_t g_rbusHandle = NULL;
static rbusDataElement_t* g_dataElements = NULL;
ElementNode **g_element_buckets = NULL;
size_t g_element_bucket_count = 0;
static volatile sig_atomic_t g_running = 1;
TableDef* g_tables = NULL;
int g_num_tables = 0;
InitialRowValue* g_initial_values = NULL;
int g_num_initial = 0;

typedef struct {
   char name[MAX_NAME_LEN];
   uint32_t max_inst;
}TableMaxInst;

static char* get_parent_table(const char* table_wild);
static char* get_parent_concrete(const char* c_table, uint32_t* p_inst);
static void ensure_table(const char* table_wild);
static int count_indices(const char* name);

// Signal handler for SIGINT and SIGTERM
static void signal_handler(int sig) {
   (void)sig;
   g_running = 0;
}

static bool is_digit_str(const char* str) {
   if (*str == '\0') return false;
   char* end;
   errno = 0;
   long val = strtol(str, &end, 10);
   return (errno == 0 && *end == '\0' && val > 0);
}

static const DataElement gDataElements[] = {
   {
      .name = "Device.DeviceInfo.SerialNumber",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_STRING,
      .value.strVal = "unknown",
      .getHandler = get_system_serial_number,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.X_COMCAST-COM_STB_IP",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_STRING,
      .value.strVal = "unknown",
      .getHandler = get_first_ip,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.X_COMCAST-COM_WAN_IP",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_STRING,
      .value.strVal = "unknown",
      .getHandler = get_first_ip,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.X_COMCAST-COM_CM_IP",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_STRING,
      .value.strVal = "unknown",
      .getHandler = get_first_ip,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.X_RDKCENTRAL-COM_SystemTime",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_STRING,
      .value.strVal = "unknown",
      .getHandler = get_system_time,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.UpTime",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_UINT,
      .value.uintVal = 0,
      .getHandler = get_system_uptime,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.X_COMCAST-COM_CM_MAC",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_STRING,
      .value.strVal = "unknown",
      .getHandler = get_mac_address,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.X_COMCAST-COM_WAN_MAC",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_STRING,
      .value.strVal = "unknown",
      .getHandler = get_mac_address,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.X_COMCAST-COM_STB_MAC",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_STRING,
      .value.strVal = "unknown",
      .getHandler = get_mac_address,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.MemoryStatus.Total",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_UINT,
      .value.uintVal = 0,
      .getHandler = get_memory_total,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.MemoryStatus.Used",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_UINT,
      .value.uintVal = 0,
      .getHandler = get_memory_used,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.MemoryStatus.Free",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_UINT,
      .value.uintVal = 0,
      .getHandler = get_memory_free,
      .setHandler = NULL,
   },
   {
      .name = "Device.DeviceInfo.ManufacturerOUI",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_STRING,
      .value.strVal = "unknown",
      .getHandler = get_manufacturer_oui,
      .setHandler = NULL,
   },
   {
      .name = "Device.Time.CurrentLocalTime",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_DATETIME,
      .value.strVal = "unknown",
      .getHandler = get_local_time,
      .setHandler = NULL,
   },
   {
      .name = "Device.SystemStatusChanged!",
      .elementType = RBUS_ELEMENT_TYPE_EVENT,
      .type = TYPE_STRING, // Not used for events
      .value.strVal = "",
      .eventSubHandler = NULL,
   }
};

static const DataElement gMethodElements[] = {
   {
      .name = "Device.Reboot()",
      .elementType = RBUS_ELEMENT_TYPE_METHOD,
      .type = TYPE_STRING, // Not used for methods
      .value.strVal = "",
      .methodHandler = system_reboot_method,
      .methodArgs = {
         .numInputArgs = 1,
         .inputArgs = (char* []){"Delay"},
         .numOutputArgs = 1,
         .outputArgs = (char* []){"Status"}
      }
   },
   {
      .name = "Device.GetSystemInfo()",
      .elementType = RBUS_ELEMENT_TYPE_METHOD,
      .type = TYPE_STRING, // Not used for methods
      .value.strVal = "",
      .methodHandler = get_system_info_method,
      .methodArgs = {
         .numInputArgs = 0,
         .inputArgs = NULL,
         .numOutputArgs = 3,
         .outputArgs = (char* []){"SerialNumber", "SystemTime", "UpTime"}
      }
   },
   {
      .name = "Device.Telemetry.Collect()",
      .elementType = RBUS_ELEMENT_TYPE_METHOD,
      .type = TYPE_STRING, // Not used for methods
      .value.strVal = "",
      .methodHandler = device_telemetry_collect,
      .methodArgs = {
         .numInputArgs = 2,
         .inputArgs = (char* []){"msg_type", "source", "dest"},
         .numOutputArgs = 1,
         .outputArgs = (char* []){"outparams"}
      }
   }
};

char* create_wildcard(const char* name) {
   if(!name || *name=='\0')
      return NULL;
   size_t len = strlen(name);
   char* result = malloc(len * 2 + 1);  // Safe upper bound for inserting {i}
   if (!result) return NULL;
   result[0] = '\0';

   bool trailing_dot = (len>0 && name[len - 1] == '.');
   char* temp = strdup(name);
   if (!temp) {
      free(result);
      return NULL;
   }

   char* token = strtok(temp, ".");
   bool first = true;
   while (token) {
      if (!first) strncat(result, ".", len * 2 + 1 - strlen(result) - 1);
      if (is_digit_str(token)) {
         strncat(result, "{i}", len * 2 + 1 - strlen(result) - 1);
      } else {
         strncat(result, token, len * 2 + 1 - strlen(result) - 1);
      }
      first = false;
      token = strtok(NULL, ".");
   }
   if (trailing_dot) strncat(result, ".", len * 2 + 1 - strlen(result) - 1);
   free(temp);
   return result;
}

char* get_parent_table(const char* table_wild) {
   const char* pattern = ".{i}.";
   const char* last = NULL;
   const char* pos = table_wild;
   while ((pos = strstr(pos, pattern)) != NULL) {
      last = pos;
      pos += 1;
   }
   if (!last) return NULL;
   size_t len = last - table_wild + 1;
   char* parent = malloc(len + 1);
   if (!parent) return NULL;
   strncpy(parent, table_wild, len);
   parent[len] = '\0';
   return parent;
}

char* get_parent_concrete(const char* c_table, uint32_t* p_inst) {
   size_t len = strlen(c_table);
   if (len < 2 || c_table[len - 1] != '.') return NULL;
   char* fake_prop = strdup(c_table);
   if (!fake_prop) return NULL;
   fake_prop[len - 1] = '\0';  // Remove trailing '.'
   char* dummy_prop = NULL;
   uint32_t dummy_inst = 0;
   char* parent_tbl = get_table_name(fake_prop, &dummy_inst, &dummy_prop);
   free(fake_prop);
   if (!parent_tbl) return NULL;
   *p_inst = dummy_inst;
   free(dummy_prop);
   return parent_tbl;
}

int count_indices(const char* name) {
   int count = 0;
   char* d = strdup(name);
   if (!d) return 0;
   char* token = strtok(d, ".");
   while (token) {
      if (is_digit_str(token)) count++;
      token = strtok(NULL, ".");
   }
   free(d);
   return count;
}

static int compare_tables(const void* a, const void* b) {
   const TableMaxInst* ta = (const TableMaxInst*)a;
   const TableMaxInst* tb = (const TableMaxInst*)b;
   int da = count_indices(ta->name);
   int db = count_indices(tb->name);
   return da - db;
}

/* ------- Hash map for DataElement lookups ------- */
static uint32_t hash_str(const char *s) {
   /* FNV-1a 32-bit */
   uint32_t h = 2166136261u;
   for (; *s; ++s) {
      h ^= (uint8_t)*s;
      h *= 16777619u;
   }
   return h;
}

void free_element_index(void) {
   if(!g_element_buckets) return;
   for(size_t i=0;i<g_element_bucket_count;i++) {
      ElementNode *n = g_element_buckets[i];
      while(n) { ElementNode *next = n->next; free(n); n = next; }
   }
   free(g_element_buckets);
   g_element_buckets = NULL;
   g_element_bucket_count = 0;
}

void build_element_index(void) {
   free_element_index();
   /* choose bucket count as next power-of-two >= elements*2 for load factor <=0.5 */
   size_t need = (size_t)g_totalElements * 2 + 1;
   size_t cap = 1;
   while(cap < need) cap <<= 1;
   if(cap < 16) cap = 16;
   g_element_bucket_count = cap;
   g_element_buckets = calloc(g_element_bucket_count, sizeof(ElementNode*));
   if(!g_element_buckets) { g_element_bucket_count = 0; return; }
   for(int i=0;i<g_totalElements;i++) {
      uint32_t h = hash_str(g_internalDataElements[i].name);
      size_t idx = h & (g_element_bucket_count - 1);
      ElementNode *node = malloc(sizeof(ElementNode));
      if(!node) continue; /* skip on OOM */
      node->key = g_internalDataElements[i].name;
      node->element = &g_internalDataElements[i];
      node->next = g_element_buckets[idx];
      g_element_buckets[idx] = node;
   }
}

DataElement *lookup_element(const char *name) {
   if(!g_element_buckets || !name) return NULL;
   uint32_t h = hash_str(name);
   size_t idx = h & (g_element_bucket_count - 1);
   ElementNode *n = g_element_buckets[idx];
   while(n) {
      if(strcmp(n->key, name)==0) return n->element;
      n = n->next;
   }
   return NULL;
}

bool loadDataElementsFromJson(const char* json_path) {
   FILE* file = fopen(json_path, "r");
   if (!file) {
      fprintf(stderr, "Failed to open JSON file: %s\n", json_path);
      return false;
   }

   fseek(file, 0, SEEK_END);
   long file_size = ftell(file);
   fseek(file, 0, SEEK_SET);
   char* json_str = (char*)malloc(file_size + 1);
   if (!json_str) {
      fprintf(stderr, "Failed to allocate memory for JSON string\n");
      fclose(file);
      return false;
   }
   size_t read_size = fread(json_str, 1, file_size, file);
   json_str[read_size] = '\0';
   fclose(file);

   cJSON* root = cJSON_Parse(json_str);
   free(json_str);
   if (!root) {
      fprintf(stderr, "Failed to parse JSON: %s\n", cJSON_GetErrorPtr());
      return false;
   }

   if (!cJSON_IsArray(root)) {
      fprintf(stderr, "JSON root is not an array\n");
      cJSON_Delete(root);
      return false;
   }

   int json_num = cJSON_GetArraySize(root);
   if (json_num == 0) {
      fprintf(stderr, "No data models found in JSON\n");
      cJSON_Delete(root);
      return false;
   }

   g_internalDataElements = NULL;
   g_numElements = 0;

   InitialRowValue* initial_values = NULL;
   int num_initial = 0;

   for (int i = 0; i < json_num; i++) {
      cJSON* item = cJSON_GetArrayItem(root, i);
      if (!cJSON_IsObject(item)) {
         fprintf(stderr, "Item %d is not an object\n", i);
         goto load_fail;
      }

      cJSON* name_obj = cJSON_GetObjectItem(item, "name");
      cJSON* element_type_obj = cJSON_GetObjectItem(item, "elementType");
      cJSON* type_obj = cJSON_GetObjectItem(item, "type");
      cJSON* value_obj = cJSON_GetObjectItem(item, "value");

      if (!cJSON_IsString(name_obj)) {
         fprintf(stderr, "Invalid name for item %d\n", i);
         goto load_fail;
      }

      const char* element_type_str;
      if (element_type_obj && cJSON_IsString(element_type_obj)) {
         element_type_str = cJSON_GetStringValue(element_type_obj);
      } else {
         element_type_str = "property";
      }

      const char* name = cJSON_GetStringValue(name_obj);
      rbusElementType_t element_type;

      if (strcmp(element_type_str, "property") == 0) {
         element_type = RBUS_ELEMENT_TYPE_PROPERTY;
      } else if (strcmp(element_type_str, "table") == 0) {
         element_type = RBUS_ELEMENT_TYPE_TABLE;
      } else if (strcmp(element_type_str, "event") == 0) {
         element_type = RBUS_ELEMENT_TYPE_EVENT;
      } else if (strcmp(element_type_str, "method") == 0) {
         element_type = RBUS_ELEMENT_TYPE_METHOD;
      } else {
         fprintf(stderr, "Invalid elementType '%s' for item %d\n", element_type_str, i);
         goto load_fail;
      }

      if (element_type == RBUS_ELEMENT_TYPE_PROPERTY) {
         if (!cJSON_IsNumber(type_obj) || (type_obj)->valuedouble < 0 || (type_obj)->valuedouble > TYPE_BYTE) {
            fprintf(stderr, "Invalid type for item %d\n", i);
            goto load_fail;
         }

         ValueType type = (ValueType)(type_obj)->valuedouble;
         uint32_t inst;
         char* prop = NULL;
         char* tbl = get_table_name(name, &inst, &prop);
         if (tbl) {
            // Row property
            InitialRowValue iv;
            strcpy(iv.table, tbl);
            iv.inst = inst;
            strcpy(iv.prop, prop);
            iv.type = type;

            switch (type) {
               case TYPE_STRING:
               case TYPE_DATETIME:
               case TYPE_BASE64:
                  iv.value.strVal = value_obj && cJSON_IsString(value_obj) ? strdup(cJSON_GetStringValue(value_obj)) : strdup("");
                  if (!iv.value.strVal) {
                     fprintf(stderr, "Failed to allocate memory for string value at item %d\n", i);
                     free(tbl);
                     free(prop);
                     goto load_fail;
                  }
                  break;
               case TYPE_INT:
                  if (value_obj && cJSON_IsNumber(value_obj)) {
                     double val = (value_obj)->valuedouble;
                     if (val >= INT32_MIN && val <= INT32_MAX) {
                        iv.value.intVal = (int32_t)val;
                     } else {
                        fprintf(stderr, "Value out of range for TYPE_INT at item %d\n", i);
                        free(tbl);
                        free(prop);
                        goto load_fail;
                     }
                  } else {
                     iv.value.intVal = 0;
                  }
                  break;
               case TYPE_UINT:
                  if (value_obj && cJSON_IsNumber(value_obj)) {
                     double val = (value_obj)->valuedouble;
                     if (val >= 0 && val <= UINT32_MAX) {
                        iv.value.uintVal = (uint32_t)val;
                     } else {
                        fprintf(stderr, "Value out of range for TYPE_UINT at item %d\n", i);
                        free(tbl);
                        free(prop);
                        goto load_fail;
                     }
                  } else {
                     iv.value.uintVal = 0;
                  }
                  break;
               case TYPE_BOOL:
                  iv.value.boolVal = value_obj && (cJSON_IsTrue(value_obj) || cJSON_IsFalse(value_obj)) ? cJSON_IsTrue(value_obj) : false;
                  break;
               case TYPE_LONG:
                  if (value_obj && cJSON_IsNumber(value_obj)) {
                     double val = (value_obj)->valuedouble;
                     if (val >= INT64_MIN && val <= INT64_MAX) {
                        iv.value.longVal = (int64_t)val;
                     } else {
                        fprintf(stderr, "Value out of range for TYPE_LONG at item %d\n", i);
                        free(tbl);
                        free(prop);
                        goto load_fail;
                     }
                  } else {
                     iv.value.longVal = 0;
                  }
                  break;
               case TYPE_ULONG:
                  if (value_obj && cJSON_IsNumber(value_obj)) {
                     double val = (value_obj)->valuedouble;
                     if (val >= 0 && val <= UINT64_MAX) {
                        iv.value.ulongVal = (uint64_t)val;
                     } else {
                        fprintf(stderr, "Value out of range for TYPE_ULONG at item %d\n", i);
                        free(tbl);
                        free(prop);
                        goto load_fail;
                     }
                  } else {
                     iv.value.ulongVal = 0;
                  }
                  break;
               case TYPE_FLOAT:
                  iv.value.floatVal = value_obj && cJSON_IsNumber(value_obj) ? (float)(value_obj)->valuedouble : 0.0f;
                  break;
               case TYPE_DOUBLE:
                  iv.value.doubleVal = value_obj && cJSON_IsNumber(value_obj) ? (value_obj)->valuedouble : 0.0;
                  break;
               case TYPE_BYTE:
                  if (value_obj && cJSON_IsNumber(value_obj)) {
                     double val = (value_obj)->valuedouble;
                     if (val >= 0 && val <= UINT8_MAX) {
                        iv.value.byteVal = (uint8_t)val;
                     } else {
                        fprintf(stderr, "Value out of range for TYPE_BYTE at item %d\n", i);
                        free(tbl);
                        free(prop);
                        goto load_fail;
                     }
                  } else {
                     iv.value.byteVal = 0;
                  }
                  break;
            }

            // Add to initial_values
            initial_values = realloc(initial_values, (num_initial + 1) * sizeof(InitialRowValue));
            initial_values[num_initial] = iv;
            num_initial++;

            // Compute wildcards
            char* table_wild = create_wildcard(tbl);
            ensure_table(table_wild);
            free(table_wild);

            // Add wildcard property if not present
            char* prop_wild = create_wildcard(name);
            bool prop_exists = false;
            for (int j = 0; j < g_numElements; j++) {
               if (strcmp(g_internalDataElements[j].name, prop_wild) == 0 && g_internalDataElements[j].elementType == RBUS_ELEMENT_TYPE_PROPERTY) {
                  prop_exists = true;
                  break;
               }
            }
            if (!prop_exists) {
               g_internalDataElements = realloc(g_internalDataElements, (g_numElements + 1) * sizeof(DataElement));
               if (!g_internalDataElements) {
                  fprintf(stderr, "Failed to allocate memory for data models\n");
                  free(tbl);
                  free(prop);
                  free(prop_wild);
                  goto load_fail;
               }

               DataElement* de = &g_internalDataElements[g_numElements];
               strncpy(de->name, prop_wild, MAX_NAME_LEN - 1);
               de->name[MAX_NAME_LEN - 1] = '\0';
               de->elementType = RBUS_ELEMENT_TYPE_PROPERTY;
               de->type = type;
               memset(&de->value, 0, sizeof(de->value));
               de->getHandler = getHandler;
               de->setHandler = setHandler;
               de->tableAddRowHandler = NULL;
               de->tableRemoveRowHandler = NULL;
               de->eventSubHandler = NULL;
               de->methodHandler = NULL;
               g_numElements++;
            }
            free(prop_wild);

            free(tbl);
            free(prop);
            continue;
         }
      }

      // Add non-row element
      g_internalDataElements = realloc(g_internalDataElements, (g_numElements + 1) * sizeof(DataElement));
      if (!g_internalDataElements) {
         fprintf(stderr, "Failed to allocate memory for data models\n");
         goto load_fail;
      }

      DataElement* de = &g_internalDataElements[g_numElements];
      strncpy(de->name, name, MAX_NAME_LEN - 1);
      de->name[MAX_NAME_LEN - 1] = '\0';
      de->elementType = element_type;
      de->getHandler = NULL;
      de->setHandler = NULL;
      de->tableAddRowHandler = NULL;
      de->tableRemoveRowHandler = NULL;
      de->eventSubHandler = NULL;
      de->methodHandler = NULL;

      if (element_type == RBUS_ELEMENT_TYPE_PROPERTY) {
         de->type = (ValueType)(type_obj)->valuedouble;

         switch (de->type) {
            case TYPE_STRING:
            case TYPE_DATETIME:
            case TYPE_BASE64:
               de->value.strVal = value_obj && cJSON_IsString(value_obj) ? strdup(cJSON_GetStringValue(value_obj)) : strdup("");
               if (!de->value.strVal) {
                  fprintf(stderr, "Failed to allocate memory for string value at item %d\n", i);
                  goto load_fail;
               }
               break;
            case TYPE_INT:
               if (value_obj && cJSON_IsNumber(value_obj)) {
                  double val = (value_obj)->valuedouble;
                  if (val >= INT32_MIN && val <= INT32_MAX) {
                     de->value.intVal = (int32_t)val;
                  } else {
                     fprintf(stderr, "Value out of range for TYPE_INT at item %d\n", i);
                     goto load_fail;
                  }
               } else {
                  de->value.intVal = 0;
               }
               break;
            case TYPE_UINT:
               if (value_obj && cJSON_IsNumber(value_obj)) {
                  double val = (value_obj)->valuedouble;
                  if (val >= 0 && val <= UINT32_MAX) {
                     de->value.uintVal = (uint32_t)val;
                  } else {
                     fprintf(stderr, "Value out of range for TYPE_UINT at item %d\n", i);
                     goto load_fail;
                  }
               } else {
                  de->value.uintVal = 0;
               }
               break;
            case TYPE_BOOL:
               de->value.boolVal = value_obj && (cJSON_IsTrue(value_obj) || cJSON_IsFalse(value_obj)) ? cJSON_IsTrue(value_obj) : false;
               break;
            case TYPE_LONG:
               if (value_obj && cJSON_IsNumber(value_obj)) {
                  double val = (value_obj)->valuedouble;
                  if (val >= INT64_MIN && val <= INT64_MAX) {
                     de->value.longVal = (int64_t)val;
                  } else {
                     fprintf(stderr, "Value out of range for TYPE_LONG at item %d\n", i);
                     goto load_fail;
                  }
               } else {
                  de->value.longVal = 0;
               }
               break;
            case TYPE_ULONG:
               if (value_obj && cJSON_IsNumber(value_obj)) {
                  double val = (value_obj)->valuedouble;
                  if (val >= 0 && val <= UINT64_MAX) {
                     de->value.ulongVal = (uint64_t)val;
                  } else {
                     fprintf(stderr, "Value out of range for TYPE_ULONG at item %d\n", i);
                     goto load_fail;
                  }
               } else {
                  de->value.ulongVal = 0;
               }
               break;
            case TYPE_FLOAT:
               de->value.floatVal = value_obj && cJSON_IsNumber(value_obj) ? (float)(value_obj)->valuedouble : 0.0f;
               break;
            case TYPE_DOUBLE:
               de->value.doubleVal = value_obj && cJSON_IsNumber(value_obj) ? (value_obj)->valuedouble : 0.0;
               break;
            case TYPE_BYTE:
               if (value_obj && cJSON_IsNumber(value_obj)) {
                  double val = (value_obj)->valuedouble;
                  if (val >= 0 && val <= UINT8_MAX) {
                     de->value.byteVal = (uint8_t)val;
                  } else {
                     fprintf(stderr, "Value out of range for TYPE_BYTE at item %d\n", i);
                     goto load_fail;
                  }
               } else {
                  de->value.byteVal = 0;
               }
               break;
         }
      } else {
         de->type = TYPE_STRING;
         de->value.strVal = strdup("");
         if (!de->value.strVal) {
            fprintf(stderr, "Failed to allocate memory for string value at item %d\n", i);
            goto load_fail;
         }
      }

      g_numElements++;
   }

   // Add hard coded
   int hard_num = sizeof(gDataElements) / sizeof(DataElement);
   g_totalElements = g_numElements + hard_num;
   void* tmp_realloc = realloc(g_internalDataElements, g_totalElements * sizeof(DataElement));
   if (!tmp_realloc) {
      fprintf(stderr, "Failed to allocate memory for data models\n");
      goto load_fail;
   }
   g_internalDataElements = tmp_realloc;

   for (int j = 0; j < hard_num; j++, g_numElements++) {
      DataElement* de = &g_internalDataElements[g_numElements];
      strncpy(de->name, gDataElements[j].name, MAX_NAME_LEN - 1);
      de->name[MAX_NAME_LEN - 1] = '\0';
      de->elementType = gDataElements[j].elementType;
      de->type = gDataElements[j].type;
      de->getHandler = gDataElements[j].getHandler;
      de->setHandler = gDataElements[j].setHandler;
      de->tableAddRowHandler = gDataElements[j].tableAddRowHandler;
      de->tableRemoveRowHandler = gDataElements[j].tableRemoveRowHandler;
      de->eventSubHandler = gDataElements[j].eventSubHandler;
      de->methodHandler = gDataElements[j].methodHandler;

      if (IS_STRING_TYPE(de->type)) {
         de->value.strVal = strdup(gDataElements[j].value.strVal);
         if (!de->value.strVal) {
            fprintf(stderr, "Failed to allocate memory for global data model string\n");
            goto load_fail;
         }
      } else {
         de->value = gDataElements[j].value;
      }
   }

   cJSON_Delete(root);

   g_initial_values = initial_values;
   g_num_initial = num_initial;

   return true;

load_fail:
   // Free allocated

   for (int j = 0; j < g_numElements; j++) {
      if (IS_STRING_TYPE(g_internalDataElements[j].type)) {
         free(g_internalDataElements[j].value.strVal);
      }
   }
   free(g_internalDataElements);
   g_internalDataElements = NULL;
   g_numElements = 0;

   for (int j = 0; j < num_initial; j++) {
      if (IS_STRING_TYPE(initial_values[j].type)) {
         free(initial_values[j].value.strVal);
      }
   }
   free(initial_values);
   cJSON_Delete(root);
   return false;
}

static void cleanup(void) {
   free_element_index();
   if (g_rbusHandle && g_dataElements && g_internalDataElements) {
      rbus_unregDataElements(g_rbusHandle, g_totalElements, g_dataElements);
      for (int i = 0; i < g_totalElements; i++) {
         if (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY ||
            g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_EVENT) {
            rbusEvent_Unsubscribe(g_rbusHandle, g_internalDataElements[i].name);
         }
         if (IS_STRING_TYPE(g_internalDataElements[i].type)) {
            free(g_internalDataElements[i].value.strVal);
         }
         free(g_dataElements[i].name);
      }
      free(g_dataElements);
      g_dataElements = NULL;
   }

   // Free tables
   for (int i = 0; i < g_num_tables; i++) {
      for (int j = 0; j < g_tables[i].num_rows; j++) {
         RowProperty* p = g_tables[i].rows[j].props;
         while (p) {
            RowProperty* next = p->next;
            if (IS_STRING_TYPE(p->type)) {
               free(p->value.strVal);
            }
            free(p);
            p = next;
         }
      }
      free(g_tables[i].rows);
   }
   free(g_tables);
   g_tables = NULL;
   g_num_tables = 0;

   if (g_rbusHandle) {
      rbus_close(g_rbusHandle);
      g_rbusHandle = NULL;
   }
}

void ensure_table(const char* table_wild) {
   if (!table_wild || strlen(table_wild) == 0) return;

   // Check if table exists
   bool exists = false;
   for (int j = 0; j < g_numElements; j++) {
      if (strcmp(g_internalDataElements[j].name, table_wild) == 0 &&
         g_internalDataElements[j].elementType == RBUS_ELEMENT_TYPE_TABLE) {
         exists = true;
         break;
      }
   }
   if (exists) return;

   // Recurse on parent
   char* parent = get_parent_table(table_wild);
   if (parent) {
      ensure_table(parent);
      free(parent);
   }

   // Add table
   void* tmp_realloc = realloc(g_internalDataElements, (g_numElements + 1) * sizeof(DataElement));
   if (!tmp_realloc) {
      fprintf(stderr, "Failed to allocate memory for data models\n");
      return;
   }
   g_internalDataElements = tmp_realloc;
   DataElement* de = &g_internalDataElements[g_numElements];
   strncpy(de->name, table_wild, MAX_NAME_LEN - 1);
   de->name[MAX_NAME_LEN - 1] = '\0';
   de->elementType = RBUS_ELEMENT_TYPE_TABLE;
   de->type = TYPE_STRING;
   de->value.strVal = strdup("");
   de->getHandler = NULL;
   de->setHandler = NULL;
   de->tableAddRowHandler = table_add_row;
   de->tableRemoveRowHandler = table_remove_row;
   de->eventSubHandler = NULL;
   de->methodHandler = NULL;
   g_numElements++;

   // Add NumberOfEntries property
   char* base = strdup(table_wild);
   if (base[strlen(base) - 1] == '.') base[strlen(base) - 1] = '\0';
   char num_name[MAX_NAME_LEN];
   snprintf(num_name, MAX_NAME_LEN, "%s%s", base, TABLE_COUNT_PROP);
   free(base);

   bool num_exists = false;
   for (int j = 0; j < g_numElements; j++) {
      if (strcmp(g_internalDataElements[j].name, num_name) == 0 &&
         g_internalDataElements[j].elementType == RBUS_ELEMENT_TYPE_PROPERTY) {
         num_exists = true;
         break;
      }
   }
   if (!num_exists) {
   void* tmp_realloc = realloc(g_internalDataElements, (g_numElements + 1) * sizeof(DataElement));
      if (!tmp_realloc) {
         fprintf(stderr, "Failed to allocate memory for data models\n");
         return;
      }
      g_internalDataElements = tmp_realloc;
      de = &g_internalDataElements[g_numElements];
      strncpy(de->name, num_name, MAX_NAME_LEN - 1);
      de->name[MAX_NAME_LEN - 1] = '\0';
      de->elementType = RBUS_ELEMENT_TYPE_PROPERTY;
      de->type = TYPE_UINT;
      de->value.uintVal = 0;
      de->getHandler = getTableHandler;
      de->setHandler = NULL;
      de->tableAddRowHandler = NULL;
      de->tableRemoveRowHandler = NULL;
      de->eventSubHandler = NULL;
      de->methodHandler = NULL;
      g_numElements++;
   }
}

static int num_table_max = 0;
static TableMaxInst* table_max = NULL;
static void update_max(const char* t_name, uint32_t inst) {
   for (int k = 0; k < num_table_max; k++) {
      if (strcmp(table_max[k].name, t_name) == 0) {
         if (inst > table_max[k].max_inst) table_max[k].max_inst = inst;
         return;
      }
   }
   table_max = realloc(table_max, (num_table_max + 1) * sizeof(TableMaxInst));
   strcpy(table_max[num_table_max].name, t_name);
   table_max[num_table_max].max_inst = inst;
   num_table_max++;
}

static void ensure_inst(const char* c_table, uint32_t c_inst) {
   if (!c_table) return;
   update_max(c_table, c_inst);
   uint32_t p_inst = 0;
   char* p_table = get_parent_concrete(c_table, &p_inst);
   if (p_table) {
      ensure_inst(p_table, p_inst);
   }
   free(p_table);
}

int main(int argc, char* argv[]) {

   // Set up signal handlers
   signal(SIGINT, signal_handler);
   signal(SIGTERM, signal_handler);
   signal(SIGHUP, signal_handler);
   signal(SIGQUIT, signal_handler);

   if (!loadDataElementsFromJson((argc == 2) ? argv[1] : JSON_FILE)) {
      fprintf(stderr, "Failed to load data elements from %s\n", (argc == 2) ? argv[1] : JSON_FILE);
      return 1;
   }

   rbusError_t rc = rbus_open(&g_rbusHandle, "rbus-dataelements");
   if (rc != RBUS_ERROR_SUCCESS) {
      fprintf(stderr, "Failed to open rbus: %d\n", rc);
      cleanup();
      return 1;
   }

   g_dataElements = (rbusDataElement_t*)malloc(g_totalElements * sizeof(rbusDataElement_t));
   if (!g_dataElements) {
      fprintf(stderr, "Failed to allocate memory for data elements\n");
      cleanup();
      return 1;
   }

   for (int i = 0; i < g_totalElements; i++) {
      g_dataElements[i].name = strdup(g_internalDataElements[i].name);
      if (!g_dataElements[i].name) {
         fprintf(stderr, "Failed to allocate memory for data element name\n");
         cleanup();
         return 1;
      }
      g_dataElements[i].type = g_internalDataElements[i].elementType;
      g_dataElements[i].cbTable.getHandler = g_internalDataElements[i].getHandler ? g_internalDataElements[i].getHandler : (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY ? getHandler : NULL);
      g_dataElements[i].cbTable.setHandler = g_internalDataElements[i].setHandler ? g_internalDataElements[i].setHandler : (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY ? setHandler : NULL);
      g_dataElements[i].cbTable.tableAddRowHandler = g_internalDataElements[i].tableAddRowHandler;
      g_dataElements[i].cbTable.tableRemoveRowHandler = g_internalDataElements[i].tableRemoveRowHandler;
      g_dataElements[i].cbTable.eventSubHandler = g_internalDataElements[i].eventSubHandler ? g_internalDataElements[i].eventSubHandler : (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_EVENT || g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY ? eventSubHandler : NULL);
   /* assign method handler after struct init; silence pedantic function pointer warning */
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
   g_dataElements[i].cbTable.methodHandler = g_internalDataElements[i].methodHandler;
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
   }

   rc = rbus_regDataElements(g_rbusHandle, g_totalElements, g_dataElements);
   if (rc != RBUS_ERROR_SUCCESS) {
      fprintf(stderr, "Failed to register data elements: %d\n", rc);
      cleanup();
      return 1;
   }

   printf("Successfully registered %d data elements\n", g_totalElements);

   build_element_index();

   for (size_t i = 0; i < sizeof(gMethodElements) / sizeof(DataElement); i++) {
      const DataElement* method = &gMethodElements[i];
      registerMethod(g_rbusHandle, method);
   }

   printf("Successfully registered %zu methods\n", sizeof(gMethodElements) / sizeof(DataElement));

   // Populate initial rows and values

   // First, collect unique concrete tables and max_inst recursively
   for (int j = 0; j < g_num_initial; j++) {
      ensure_inst(g_initial_values[j].table, g_initial_values[j].inst);
   }

   // Sort by increasing number of indices (outer first)
   qsort(table_max, num_table_max, sizeof(TableMaxInst), compare_tables);

   // Add initial rows
   for (int k = 0; k < num_table_max; k++) {
      char* tbl = table_max[k].name;
      int max = table_max[k].max_inst;

      // Find or create TableDef
      TableDef* table = NULL;
      for (int i = 0; i < g_num_tables; i++) {
         if (strcmp(g_tables[i].name, tbl) == 0) {
            table = &g_tables[i];
            break;
         }
      }
      if (!table) {
         g_tables = realloc(g_tables, (g_num_tables + 1) * sizeof(TableDef));
         table = &g_tables[g_num_tables++];
         strcpy(table->name, tbl);
         table->rows = NULL;
         table->num_rows = 0;
         table->next_inst = 1;
         table->num_inst = 0;
      }

   for (uint32_t m = table->next_inst; m <= (uint32_t)max; m++) {
         table->rows = realloc(table->rows, (table->num_rows + 1) * sizeof(TableRow));
         TableRow* row = &table->rows[table->num_rows];
         snprintf(row->name, MAX_NAME_LEN, "%s", tbl);
         row->instNum = m;
         row->alias[0] = '\0';
         row->props = NULL;
         table->num_rows++;
         table->num_inst++; /* ensure getTableHandler returns correct NumberOfEntries */

         //rc = rbusTable_registerRow(g_rbusHandle, row->name, row->instNum, NULL);
         rc = rbusTable_addRow(g_rbusHandle, row->name, row->alias, &row->instNum);
         if (rc != RBUS_ERROR_SUCCESS) {
            fprintf(stderr, "Failed to register initial row %s: %d\n", row->name, rc);
         }
      }
      table->next_inst = max + 1;
   }
   free(table_max);

   // Set initial values
   for (int j = 0; j < g_num_initial; j++) {
      /* Allocate enough space: table + digits (max 10 for 32-bit) + dot + prop + null */
      char concrete[MAX_NAME_LEN * 2];
      snprintf(concrete, sizeof(concrete), "%s%u.%s", g_initial_values[j].table, g_initial_values[j].inst, g_initial_values[j].prop);

      rbusValue_t val;
      rbusValue_Init(&val);
      switch (g_initial_values[j].type) {
         case TYPE_STRING:
         case TYPE_DATETIME:
         case TYPE_BASE64:
            rbusValue_SetString(val, g_initial_values[j].value.strVal);
            break;
         case TYPE_INT:
            rbusValue_SetInt32(val, g_initial_values[j].value.intVal);
            break;
         case TYPE_UINT:
            rbusValue_SetUInt32(val, g_initial_values[j].value.uintVal);
            break;
         case TYPE_BOOL:
            rbusValue_SetBoolean(val, g_initial_values[j].value.boolVal);
            break;
         case TYPE_LONG:
            rbusValue_SetInt64(val, g_initial_values[j].value.longVal);
            break;
         case TYPE_ULONG:
            rbusValue_SetUInt64(val, g_initial_values[j].value.ulongVal);
            break;
         case TYPE_FLOAT:
            rbusValue_SetSingle(val, g_initial_values[j].value.floatVal);
            break;
         case TYPE_DOUBLE:
            rbusValue_SetDouble(val, g_initial_values[j].value.doubleVal);
            break;
         case TYPE_BYTE:
            rbusValue_SetByte(val, g_initial_values[j].value.byteVal);
            break;
      }

      rbusSetOptions_t opts = {.commit = true};
      rc = rbus_set(g_rbusHandle, concrete, val, &opts);
      if (rc != RBUS_ERROR_SUCCESS) {
         fprintf(stderr, "Failed to set initial value for %s: %d\n", concrete, rc);
      }
      rbusValue_Release(val);
   }

   // Free initial
   for (int j = 0; j < g_num_initial; j++) {
      if (IS_STRING_TYPE(g_initial_values[j].type)) {
         free(g_initial_values[j].value.strVal);
      }
   }
   free(g_initial_values);
   g_initial_values = NULL;
   g_num_initial = 0;

   // Set non-table properties
   for (int i = 0; i < g_totalElements; i++) {
      if (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY) {
         if (strstr(g_internalDataElements[i].name, "{i}") != NULL) {
            continue; // Skip wildcard properties
         }
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

         rbusSetOptions_t opts = {.commit = true};
         //fprintf(stdout, "Setting initial value for %s\n", g_internalDataElements[i].name);
         rc = rbus_set(g_rbusHandle, g_internalDataElements[i].name, value, &opts);
         if (rc != RBUS_ERROR_SUCCESS) {
            fprintf(stderr, "Failed to set %s: %d\n", g_internalDataElements[i].name, rc);
         }
         rbusValue_Release(value);
      }
   }

   system("touch /tmp/pam_initialized");

   while (g_running) {
      sleep(1);
   }

   fprintf(stdout, "Shutting down...\n");
   cleanup();
   return 0;
}
