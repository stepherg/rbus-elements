#include <rbus.h>
#include <cJSON.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <signal.h>
#include <time.h>
#include <stdint.h>

#ifdef __APPLE__
#include <sys/sysctl.h>
#include <IOKit/IOKitLib.h>
#include <CoreFoundation/CoreFoundation.h>
#include <mach/mach.h>
#include <mach/vm_statistics.h>
#else
#include <sys/ioctl.h>
#include <net/if.h>
#include <unistd.h>
#include <netinet/in.h>
#include <errno.h>
#endif

#define MAX_NAME_LEN 512
#define JSON_FILE "elements.json"
#define MEMORY_CACHE_TIMEOUT 5
#define MAX_REGISTERED_EVENTS 10
#define TABLE_COUNT_PROP "NumberOfEntries"

typedef enum {
   TYPE_STRING = 0,
   TYPE_INT = 1,
   TYPE_UINT = 2,
   TYPE_BOOL = 3,
   TYPE_DATETIME = 4,
   TYPE_BASE64 = 5,
   TYPE_LONG = 6,
   TYPE_ULONG = 7,
   TYPE_FLOAT = 8,
   TYPE_DOUBLE = 9,
   TYPE_BYTE = 10
} ValueType;

typedef struct {
   char name[MAX_NAME_LEN];
   rbusElementType_t elementType; // RBUS_ELEMENT_TYPE_PROPERTY, TABLE, EVENT, or METHOD
   ValueType type; // Used for properties only
   union {
      char *strVal;          // TYPE_STRING, TYPE_DATETIME, TYPE_BASE64
      int32_t intVal;        // TYPE_INT
      uint32_t uintVal;      // TYPE_UINT
      bool boolVal;          // TYPE_BOOL
      int64_t longVal;       // TYPE_LONG
      uint64_t ulongVal;     // TYPE_ULONG
      float floatVal;        // TYPE_FLOAT
      double doubleVal;      // TYPE_DOUBLE
      uint8_t byteVal;       // TYPE_BYTE
   } value;
   rbusGetHandler_t getHandler;
   rbusSetHandler_t setHandler;
   rbusTableAddRowHandler_t tableAddRowHandler;
   rbusTableRemoveRowHandler_t tableRemoveRowHandler;
   rbusEventSubHandler_t eventSubHandler;
   rbusMethodHandler_t methodHandler;
} DataElement;

typedef struct RowProperty {
   char name[MAX_NAME_LEN];
   ValueType type;
   union {
      char *strVal;
      int32_t intVal;
      uint32_t uintVal;
      bool boolVal;
      int64_t longVal;
      uint64_t ulongVal;
      float floatVal;
      double doubleVal;
      uint8_t byteVal;
   } value;
   struct RowProperty *next;
} RowProperty;

typedef struct {
   char name[MAX_NAME_LEN];
   uint32_t instNum;
   char alias[MAX_NAME_LEN];
   RowProperty *props;
} TableRow;

typedef struct {
   char name[MAX_NAME_LEN];
   TableRow *rows;
   int num_rows;
   uint32_t next_inst;
   uint32_t num_inst;
} TableDef;

typedef struct {
   char table[MAX_NAME_LEN];
   int inst;
   char prop[MAX_NAME_LEN];
   ValueType type;
   union {
      char *strVal;
      int32_t intVal;
      uint32_t uintVal;
      bool boolVal;
      int64_t longVal;
      uint64_t ulongVal;
      float floatVal;
      double doubleVal;
      uint8_t byteVal;
   } value;
} InitialRowValue;

// Built-in DeviceInfo data models
rbusError_t get_system_serial_number(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_system_time(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_system_uptime(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_mac_address(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_memory_free(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_memory_used(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_memory_total(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_local_time(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_manufacturer_oui(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);


// Methods
rbusError_t system_reboot_method(rbusHandle_t handle, const char *methodName, rbusObject_t inParams, rbusObject_t outParams, rbusMethodAsyncHandle_t asyncHandle);
rbusError_t get_system_info_method(rbusHandle_t handle, const char *methodName, rbusObject_t inParams, rbusObject_t outParams, rbusMethodAsyncHandle_t asyncHandle);
rbusError_t device_x_rdk_xmidt_send_data(rbusHandle_t handle, const char *methodName, rbusObject_t inParams, rbusObject_t outParams, rbusMethodAsyncHandle_t asyncHandle);

// Handlers
char *get_table_name(const char *name, uint32_t *instance, char **property_name);
rbusError_t getTableHandler(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t table_add_row(rbusHandle_t handle, const char *tableName, const char *aliasName, uint32_t *instNum);
rbusError_t table_remove_row(rbusHandle_t handle, const char *rowName);
void valueChangeHandler(rbusHandle_t handle, rbusEvent_t const *event, rbusEventSubscription_t *subscription);
rbusError_t eventSubHandler(rbusHandle_t handle, rbusEventSubAction_t action, const char *eventName, rbusFilter_t filter, int32_t interval, bool *autoPublish);
rbusError_t getHandler(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t setHandler(rbusHandle_t handle, rbusProperty_t property, rbusSetHandlerOptions_t *options);

#define IS_STRING_TYPE(type) (type == TYPE_STRING || type == TYPE_DATETIME || type == TYPE_BASE64)

char *create_wildcard(const char *name);
