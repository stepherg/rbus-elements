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
#include <limits.h>
#endif

#define MAX_NAME_LEN 256
#define JSON_FILE "elements.json"
#define MEMORY_CACHE_TIMEOUT 5
#define MAX_REGISTERED_EVENTS 10

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


rbusError_t get_system_serial_number(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_system_time(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_system_uptime(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_mac_address(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_memory_free(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_memory_used(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_memory_total(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_local_time(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
rbusError_t get_manufacturer_oui(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options);
