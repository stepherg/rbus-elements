#include "rbus_elements.h"
#include "model_alloc.h"
#include "model_value.h"
#include "provider_state.h"
#include <fcntl.h>
#include <math.h>
#include <sys/stat.h>

DataElement* g_internalDataElements = NULL;
static int g_numElements = 0;
int g_totalElements = 0;
rbusHandle_t g_rbusHandle = NULL;
static rbusDataElement_t* g_dataElements = NULL;
static int g_numDataElementNames = 0;
static bool g_elementsRegistered = false;
static size_t g_numMethodsRegistered = 0;
ElementNode **g_element_buckets = NULL;
size_t g_element_bucket_count = 0;
static volatile sig_atomic_t g_running = 1;
TableDef* g_tables = NULL;
int g_num_tables = 0;
InitialRowValue* g_initial_values = NULL;
int g_num_initial = 0;

typedef struct {
   char name[MAX_NAME_LEN];
   uint32_t inst;
}TableMaxInst;

static char* get_parent_table(const char* table_wild);
static char* get_parent_concrete(const char* c_table, uint32_t* p_inst);
static bool ensure_table(const char* table_wild);
static int count_indices(const char* name);

static DataElement* find_data_element(const char* name) {
   for (int i = 0; i < g_numElements; i++) {
      if (strcmp(g_internalDataElements[i].name, name) == 0) return &g_internalDataElements[i];
   }
   return NULL;
}

static DataElement* append_data_element(void) {
   DataElement* resized = model_realloc(g_internalDataElements, (size_t)(g_numElements + 1) * sizeof(*resized));
   if (!resized) return NULL;
   g_internalDataElements = resized;
   DataElement* element = &g_internalDataElements[g_numElements++];
   memset(element, 0, sizeof(*element));
   return element;
}

static bool append_initial_value(InitialRowValue** values, int* count, const InitialRowValue* value) {
   InitialRowValue* resized = model_realloc(*values, (size_t)(*count + 1) * sizeof(*resized));
   if (!resized) return false;
   *values = resized;
   (*values)[(*count)++] = *value;
   return true;
}

static bool model_name_is_valid(const char* name) {
   size_t length = name ? strlen(name) : 0;
   return length > 0 && length < MAX_NAME_LEN;
}

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
      .name = "SetPSMRecordValue()",
      .elementType = RBUS_ELEMENT_TYPE_METHOD,
      .type = TYPE_STRING,
      .value.strVal = "",
      .methodHandler = psm_set_record_value_method,
      .methodArgs = {
         .numInputArgs = 0,
         .inputArgs = NULL,
         .numOutputArgs = 0,
         .outputArgs = NULL
      }
   },
   {
      .name = "GetPSMRecordValue()",
      .elementType = RBUS_ELEMENT_TYPE_METHOD,
      .type = TYPE_STRING,
      .value.strVal = "",
      .methodHandler = psm_get_record_value_method,
      .methodArgs = {
         .numInputArgs = 0,
         .inputArgs = NULL,
         .numOutputArgs = 0,
         .outputArgs = NULL
      }
   },
   {
      .name = "Device.Telemetry.Collect()",
      .elementType = RBUS_ELEMENT_TYPE_METHOD,
      .type = TYPE_STRING, // Not used for methods
      .value.strVal = "",
      .methodHandler = device_telemetry_collect,
      .methodArgs = {
         .numInputArgs = 12,
         .inputArgs = (char* []){"msg_type", "source", "dest", "content_type", "partner_ids", "headers", "metadata", "payload", "session_id", "transaction_uuid", "qos", "rdr"},
         .numOutputArgs = 1,
         .outputArgs = (char* []){"status"}
      }
   }
};

static const DataElement* find_builtin_method(const char* name) {
   for (size_t i = 0; i < sizeof(gMethodElements) / sizeof(gMethodElements[0]); i++) {
      if (strcmp(gMethodElements[i].name, name) == 0) return &gMethodElements[i];
   }
   return NULL;
}

char* create_wildcard(const char* name) {
   if(!name || *name=='\0')
      return NULL;
   size_t len = strlen(name);
   char* result = model_malloc(len * 2 + 1);  // Safe upper bound for inserting {i}
   if (!result) return NULL;
   result[0] = '\0';

   bool trailing_dot = (len>0 && name[len - 1] == '.');
   char* temp = model_strdup(name);
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
   char* parent = model_malloc(len + 1);
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
   if (da != db) return da - db;
   int names = strcmp(ta->name, tb->name);
   if (names != 0) return names;
   return ta->inst < tb->inst ? -1 : ta->inst > tb->inst;
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

bool build_element_index(void) {
   free_element_index();
   /* choose bucket count as next power-of-two >= elements*2 for load factor <=0.5 */
   size_t need = (size_t)g_totalElements * 2 + 1;
   size_t cap = 1;
   while(cap < need) cap <<= 1;
   if(cap < 16) cap = 16;
   g_element_bucket_count = cap;
   g_element_buckets = calloc(g_element_bucket_count, sizeof(ElementNode*));
   if(!g_element_buckets) { g_element_bucket_count = 0; return false; }
   for(int i=0;i<g_totalElements;i++) {
      uint32_t h = hash_str(g_internalDataElements[i].name);
      size_t idx = h & (g_element_bucket_count - 1);
      ElementNode *node = malloc(sizeof(ElementNode));
      if(!node) {
         free_element_index();
         return false;
      }
      node->key = g_internalDataElements[i].name;
      node->element = &g_internalDataElements[i];
      node->next = g_element_buckets[idx];
      g_element_buckets[idx] = node;
   }
   return true;
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
   char* json_str = model_malloc((size_t)file_size + 1);
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
   char** source_names = model_calloc((size_t)json_num, sizeof(*source_names));
   int num_source_names = 0;
   if (!source_names) {
      fprintf(stderr, "Failed to allocate model identity set\n");
      cJSON_Delete(root);
      return false;
   }

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
      if (!model_name_is_valid(name)) {
         fprintf(stderr, "Invalid name for item %d ('%s'): name must contain 1-%d bytes\n",
            i, name ? name : "", MAX_NAME_LEN - 1);
         goto load_fail;
      }
      for (int j = 0; j < num_source_names; j++) {
         if (strcmp(source_names[j], name) == 0) {
            fprintf(stderr, "Duplicate name for item %d ('%s')\n", i, name);
            goto load_fail;
         }
      }
      source_names[num_source_names] = model_strdup(name);
      if (!source_names[num_source_names++]) {
         fprintf(stderr, "Failed to allocate name for item %d ('%s')\n", i, name);
         goto load_fail;
      }
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

      if (element_type == RBUS_ELEMENT_TYPE_METHOD) {
         if (!find_builtin_method(name)) {
            fprintf(stderr, "Unsupported method for item %d ('%s')\n", i, name);
            goto load_fail;
         }
         continue;
      }

      if (element_type == RBUS_ELEMENT_TYPE_PROPERTY) {
         if (!cJSON_IsNumber(type_obj) || trunc(type_obj->valuedouble) != type_obj->valuedouble ||
            type_obj->valuedouble < 0 || type_obj->valuedouble > TYPE_BYTE) {
            fprintf(stderr, "Invalid type for item %d ('%s')\n", i, name);
            goto load_fail;
         }

         ValueType type = (ValueType)(type_obj)->valuedouble;
         uint32_t inst;
         char* prop = NULL;
         char* tbl = get_table_name(name, &inst, &prop);
         if (tbl) {
            if (!model_name_is_valid(tbl) || !model_name_is_valid(prop)) {
               fprintf(stderr, "Synthesized name exceeds limit for item %d ('%s')\n", i, name);
               free(tbl);
               free(prop);
               goto load_fail;
            }
            // Row property
            InitialRowValue iv;
            snprintf(iv.table, sizeof(iv.table), "%s", tbl);
            iv.inst = inst;
            snprintf(iv.prop, sizeof(iv.prop), "%s", prop);
            iv.type = type;
            char value_error[128];
            if (!model_value_parse(value_obj, type, &iv.value, value_error, sizeof(value_error))) {
               fprintf(stderr, "Invalid value for item %d ('%s'): %s\n", i, name, value_error);
               free(tbl);
               free(prop);
               goto load_fail;
            }

            // Add to initial_values
            if (!append_initial_value(&initial_values, &num_initial, &iv)) {
               fprintf(stderr, "Failed to grow initial values for item %d ('%s')\n", i, name);
               model_value_free(type, &iv.value);
               free(tbl);
               free(prop);
               goto load_fail;
            }

            // Compute wildcards
            char* table_wild = create_wildcard(tbl);
            if (!table_wild || !model_name_is_valid(table_wild) || !ensure_table(table_wild)) {
               fprintf(stderr, "Failed to synthesize table for item %d ('%s')\n", i, name);
               free(table_wild);
               free(tbl);
               free(prop);
               goto load_fail;
            }
            free(table_wild);

            // Add wildcard property if not present
            char* prop_wild = create_wildcard(name);
            if (!prop_wild || !model_name_is_valid(prop_wild)) {
               fprintf(stderr, "Invalid synthesized property for item %d ('%s')\n", i, name);
               free(tbl);
               free(prop);
               free(prop_wild);
               goto load_fail;
            }
            DataElement* existing = find_data_element(prop_wild);
            if (existing && (existing->elementType != RBUS_ELEMENT_TYPE_PROPERTY || existing->type != type)) {
               fprintf(stderr, "Conflicting synthesized name for item %d ('%s'): %s\n", i, name, prop_wild);
               free(tbl);
               free(prop);
               free(prop_wild);
               goto load_fail;
            }
            if (!existing) {
               DataElement* de = append_data_element();
               if (!de) {
                  fprintf(stderr, "Failed to allocate memory for data models\n");
                  free(tbl);
                  free(prop);
                  free(prop_wild);
                  goto load_fail;
               }
               snprintf(de->name, sizeof(de->name), "%s", prop_wild);
               de->elementType = RBUS_ELEMENT_TYPE_PROPERTY;
               de->type = type;
               de->getHandler = getHandler;
               de->setHandler = setHandler;
            }
            free(prop_wild);

            free(tbl);
            free(prop);
            continue;
         }
      }

      // Add non-row element
      if (find_data_element(name)) {
         fprintf(stderr, "Conflicting configured or synthesized name for item %d ('%s')\n", i, name);
         goto load_fail;
      }
      DataElement* de = append_data_element();
      if (!de) {
         fprintf(stderr, "Failed to allocate memory for data models\n");
         goto load_fail;
      }

      snprintf(de->name, sizeof(de->name), "%s", name);
      de->elementType = element_type;

      if (element_type == RBUS_ELEMENT_TYPE_PROPERTY) {
         de->type = (ValueType)(type_obj)->valuedouble;
         char value_error[128];
         if (!model_value_parse(value_obj, de->type, &de->value, value_error, sizeof(value_error))) {
            fprintf(stderr, "Invalid value for item %d ('%s'): %s\n", i, name, value_error);
            goto load_fail;
         }
      } else {
         de->type = TYPE_STRING;
         de->value.strVal = model_strdup("");
         if (!de->value.strVal) {
            fprintf(stderr, "Failed to allocate memory for string value at item %d\n", i);
            goto load_fail;
         }
      }

   }

   // Add hard coded
   int hard_num = sizeof(gDataElements) / sizeof(DataElement);
   for (int j = 0; j < hard_num; j++) {
      if (find_data_element(gDataElements[j].name)) {
         fprintf(stderr, "Configured name conflicts with built-in '%s'\n", gDataElements[j].name);
         goto load_fail;
      }
      DataElement* de = append_data_element();
      if (!de) {
         fprintf(stderr, "Failed to grow model for built-in '%s'\n", gDataElements[j].name);
         goto load_fail;
      }
      snprintf(de->name, sizeof(de->name), "%s", gDataElements[j].name);
      de->elementType = gDataElements[j].elementType;
      de->type = gDataElements[j].type;
      de->getHandler = gDataElements[j].getHandler;
      de->setHandler = gDataElements[j].setHandler;
      de->tableAddRowHandler = gDataElements[j].tableAddRowHandler;
      de->tableRemoveRowHandler = gDataElements[j].tableRemoveRowHandler;
      de->eventSubHandler = gDataElements[j].eventSubHandler;
      de->methodHandler = gDataElements[j].methodHandler;

      if (IS_STRING_TYPE(de->type)) {
         de->value.strVal = model_strdup(gDataElements[j].value.strVal);
         if (!de->value.strVal) {
            fprintf(stderr, "Failed to allocate memory for global data model string\n");
            goto load_fail;
         }
      } else {
         de->value = gDataElements[j].value;
      }
   }

   g_totalElements = g_numElements;

   cJSON_Delete(root);
   for (int i = 0; i < num_source_names; i++) free(source_names[i]);
   free(source_names);

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
   for (int i = 0; i < num_source_names; i++) free(source_names[i]);
   free(source_names);
   cJSON_Delete(root);
   return false;
}

static void cleanup(void) {
   shutdown_psm();
   free_element_index();
   if (g_rbusHandle) {
      for (size_t i = 0; i < g_numMethodsRegistered; i++) {
         rbusDataElement_t method = {(char*)gMethodElements[i].name, RBUS_ELEMENT_TYPE_METHOD, {0}};
         rbus_unregDataElements(g_rbusHandle, 1, &method);
      }
      g_numMethodsRegistered = 0;
   }
   if (g_rbusHandle && g_elementsRegistered && g_dataElements) {
      rbus_unregDataElements(g_rbusHandle, g_totalElements, g_dataElements);
      g_elementsRegistered = false;
   }
   if (g_rbusHandle && g_internalDataElements) {
      for (int i = 0; i < g_numElements; i++) {
         if (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY ||
            g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_EVENT) {
            rbusEvent_Unsubscribe(g_rbusHandle, g_internalDataElements[i].name);
         }
      }
   }
   if (g_dataElements) {
      for (int i = 0; i < g_numDataElementNames; i++) free(g_dataElements[i].name);
      free(g_dataElements);
      g_dataElements = NULL;
      g_numDataElementNames = 0;
   }
   for (int i = 0; i < g_numElements; i++) {
      model_value_free(g_internalDataElements[i].type, &g_internalDataElements[i].value);
   }
   free(g_internalDataElements);
   g_internalDataElements = NULL;
   g_numElements = 0;
   g_totalElements = 0;

   for (int i = 0; i < g_num_initial; i++) {
      model_value_free(g_initial_values[i].type, &g_initial_values[i].value);
   }
   free(g_initial_values);
   g_initial_values = NULL;
   g_num_initial = 0;

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

bool ensure_table(const char* table_wild) {
   if (!model_name_is_valid(table_wild)) return false;

   // Check if table exists
   bool exists = false;
   for (int j = 0; j < g_numElements; j++) {
      if (strcmp(g_internalDataElements[j].name, table_wild) == 0 &&
         g_internalDataElements[j].elementType == RBUS_ELEMENT_TYPE_TABLE) {
         exists = true;
         break;
      }
   }
   if (exists) return true;

   // Recurse on parent
   char* parent = get_parent_table(table_wild);
   if (parent) {
      if (!ensure_table(parent)) {
         free(parent);
         return false;
      }
      free(parent);
   }

   // Add table
   DataElement* de = append_data_element();
   if (!de) return false;
   snprintf(de->name, sizeof(de->name), "%s", table_wild);
   de->elementType = RBUS_ELEMENT_TYPE_TABLE;
   de->type = TYPE_STRING;
   de->value.strVal = strdup("");
   de->getHandler = NULL;
   de->setHandler = NULL;
   de->tableAddRowHandler = table_add_row;
   de->tableRemoveRowHandler = table_remove_row;

   // Add NumberOfEntries property
   char* base = model_strdup(table_wild);
   if (!base) return false;
   if (base[strlen(base) - 1] == '.') base[strlen(base) - 1] = '\0';
   char num_name[MAX_NAME_LEN];
   int written = snprintf(num_name, sizeof(num_name), "%s%s", base, TABLE_COUNT_PROP);
   free(base);
   if (written < 0 || (size_t)written >= sizeof(num_name)) return false;

   bool num_exists = false;
   for (int j = 0; j < g_numElements; j++) {
      if (strcmp(g_internalDataElements[j].name, num_name) == 0 &&
         g_internalDataElements[j].elementType == RBUS_ELEMENT_TYPE_PROPERTY) {
         num_exists = true;
         break;
      }
   }
   if (!num_exists) {
      de = append_data_element();
      if (!de) return false;
      snprintf(de->name, sizeof(de->name), "%s", num_name);
      de->elementType = RBUS_ELEMENT_TYPE_PROPERTY;
      de->type = TYPE_UINT;
      de->value.uintVal = 0;
      de->getHandler = getTableHandler;
      de->setHandler = NULL;
   }
   return true;
}

static int num_table_max = 0;
static TableMaxInst* table_max = NULL;
static bool update_max(const char* t_name, uint32_t inst) {
   for (int k = 0; k < num_table_max; k++) {
      if (strcmp(table_max[k].name, t_name) == 0 && table_max[k].inst == inst) return true;
   }
   TableMaxInst* resized = realloc(table_max, (size_t)(num_table_max + 1) * sizeof(*resized));
   if (!resized) return false;
   table_max = resized;
   snprintf(table_max[num_table_max].name, sizeof(table_max[num_table_max].name), "%s", t_name);
   table_max[num_table_max].inst = inst;
   num_table_max++;
   return true;
}

static bool ensure_inst(const char* c_table, uint32_t c_inst) {
   if (!c_table || !update_max(c_table, c_inst)) return false;
   uint32_t p_inst = 0;
   char* p_table = get_parent_concrete(c_table, &p_inst);
   bool success = !p_table || ensure_inst(p_table, p_inst);
   free(p_table);
   return success;
}

static bool publish_readiness(const char* path) {
   int descriptor = open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
   if (descriptor < 0) return false;
   bool success = fsync(descriptor) == 0;
   if (close(descriptor) != 0) success = false;
   if (!success) {
      unlink(path);
   }
   return success;
}

int main(int argc, char* argv[]) {

   // Set up signal handlers
   signal(SIGINT, signal_handler);
   signal(SIGTERM, signal_handler);
   signal(SIGHUP, signal_handler);
   signal(SIGQUIT, signal_handler);

   bool validate_only = argc == 3 && strcmp(argv[1], "--validate") == 0;
   const char* json_path = validate_only ? argv[2] : ((argc == 2) ? argv[1] : JSON_FILE);
   if (!loadDataElementsFromJson(json_path)) {
      fprintf(stderr, "Failed to load data elements from %s\n", json_path);
      return 1;
   }
   if (validate_only) {
      cleanup();
      return 0;
   }

   const char* readiness_path = getenv("RBUS_ELEMENTS_READY_PATH");
   if (!readiness_path || !*readiness_path) readiness_path = "/tmp/pam_initialized";
   if (unlink(readiness_path) != 0 && errno != ENOENT) {
      fprintf(stderr, "Failed to clear readiness marker %s: %s\n", readiness_path, strerror(errno));
      cleanup();
      return 1;
   }

   if (!initialize_psm()) {
      cleanup();
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
      g_numDataElementNames++;
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
   g_elementsRegistered = true;

   printf("Successfully registered %d data elements\n", g_totalElements);

   if (!build_element_index()) {
      fprintf(stderr, "Failed to build element index\n");
      cleanup();
      return 1;
   }

   for (size_t i = 0; i < sizeof(gMethodElements) / sizeof(DataElement); i++) {
      const DataElement* method = &gMethodElements[i];
      rc = registerMethod(g_rbusHandle, method);
      if (rc != RBUS_ERROR_SUCCESS) {
         fprintf(stderr, "Failed to register required method %s: %d\n", method->name, rc);
         cleanup();
         return 1;
      }
      g_numMethodsRegistered++;
   }

   printf("Successfully registered %zu methods\n", sizeof(gMethodElements) / sizeof(DataElement));

   // Populate initial rows and values

   // First, collect unique concrete table instances recursively
   for (int j = 0; j < g_num_initial; j++) {
      if (!ensure_inst(g_initial_values[j].table, g_initial_values[j].inst)) {
         fprintf(stderr, "Failed to plan initial row %s%u\n", g_initial_values[j].table, g_initial_values[j].inst);
         cleanup();
         return 1;
      }
   }

   // Sort by increasing number of indices (outer first)
   qsort(table_max, num_table_max, sizeof(TableMaxInst), compare_tables);

   // Add initial rows
   for (int k = 0; k < num_table_max; k++) {
      char* tbl = table_max[k].name;
      uint32_t instance = table_max[k].inst;
      rc = rbusTable_registerRow(g_rbusHandle, tbl, instance, NULL);
      if (rc != RBUS_ERROR_SUCCESS) {
         fprintf(stderr, "Failed to register initial row %s%u: %d\n", tbl, instance, rc);
         free(table_max);
         table_max = NULL;
         num_table_max = 0;
         cleanup();
         return 1;
      }
      rc = provider_table_commit_registered(tbl, instance, NULL);
      if (rc != RBUS_ERROR_SUCCESS) {
         char row_name[MAX_NAME_LEN];
         snprintf(row_name, sizeof(row_name), "%s%u", tbl, instance);
         rbusTable_unregisterRow(g_rbusHandle, row_name);
         fprintf(stderr, "Failed to commit initial row %s: %d\n", row_name, rc);
         free(table_max);
         table_max = NULL;
         num_table_max = 0;
         cleanup();
         return 1;
      }
   }
   free(table_max);
   table_max = NULL;
   num_table_max = 0;

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
         rbusValue_Release(val);
         cleanup();
         return 1;
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
            rbusValue_Release(value);
            cleanup();
            return 1;
         }
         rbusValue_Release(value);
      }
   }

   if (!publish_readiness(readiness_path)) {
      fprintf(stderr, "Failed to publish readiness marker %s: %s\n", readiness_path, strerror(errno));
      cleanup();
      return 1;
   }

   while (g_running) {
      sleep(1);
   }

   fprintf(stdout, "Shutting down...\n");
   cleanup();
   return 0;
}
