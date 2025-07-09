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

// Memory cache for optimization
typedef struct {
   uint64_t total;  // Total memory in kB
   uint64_t free;   // Free memory in kB
   uint64_t used;   // Used memory in kB
   time_t last_updated; // Last update timestamp
} MemoryCache;

static DataElement *g_internalDataElements = NULL;
static int g_numElements = 0;
static int g_totalElements = 0;
static rbusHandle_t g_rbusHandle = NULL;
static rbusDataElement_t *g_dataElements = NULL;
volatile sig_atomic_t g_running = 1;
static MemoryCache g_mem_cache = {0};

// Signal handler for SIGINT and SIGTERM
static void signal_handler(int sig) {
   g_running = 0;
}

static rbusError_t eventSubHandler(rbusHandle_t handle, rbusEventSubAction_t action, const char *eventName, rbusFilter_t filter, int32_t interval, bool *autoPublish);
// Platform-specific implementation to retrieve the system serial number:
// On macOS, fetches the hardware serial number; on other platforms, uses the MAC address as a fallback identifier.
static rbusError_t get_system_serial_number(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options) {
   rbusValue_t value;
   rbusValue_Init(&value);

#ifdef __APPLE__
   io_service_t platformExpert = IOServiceGetMatchingService(kIOMainPortDefault,
      IOServiceMatching("IOPlatformExpertDevice"));
   if (!platformExpert) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   CFStringRef serialNumber = IORegistryEntryCreateCFProperty(platformExpert,
      CFSTR(kIOPlatformSerialNumberKey),
      kCFAllocatorDefault,
      0);

   IOObjectRelease(platformExpert);

   if (!serialNumber) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   CFIndex length = CFStringGetLength(serialNumber);
   CFIndex maxSize = CFStringGetMaximumSizeForEncoding(length, kCFStringEncodingUTF8) + 1;

   if (maxSize <= 0 || maxSize > 4096) { // 4096 is an arbitrary upper bound for safety
      CFRelease(serialNumber);
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   char *serial = (char *)malloc(maxSize);
   if (!serial) {
      CFRelease(serialNumber);
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   if (!CFStringGetCString(serialNumber, serial, maxSize, kCFStringEncodingUTF8)) {
      free(serial);
      CFRelease(serialNumber);
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   CFRelease(serialNumber);
   rbusValue_SetString(value, serial);
   free(serial);

#else
   // On non-Apple platforms, use the MAC address of the first non-loopback interface as a fallback "serial number".
   // This is not a true serial number, but serves as a unique identifier if no hardware serial is available.
   int sock = socket(AF_INET, SOCK_DGRAM, 0);
   if (sock < 0) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   struct ifreq ifr;
   struct ifconf ifc;
   char buf[1024];
   char mac_str[18] = {0};
   bool found = false;

   ifc.ifc_len = sizeof(buf);
   ifc.ifc_buf = buf;
   if (ioctl(sock, SIOCGIFCONF, &ifc) < 0) {
      close(sock);
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   struct ifreq *it = ifc.ifc_req;
   const struct ifreq *const end = it + (ifc.ifc_len / sizeof(struct ifreq));

   for (; it != end; ++it) {
      strcpy(ifr.ifr_name, it->ifr_name);
      if (ioctl(sock, SIOCGIFFLAGS, &ifr) == 0) {
         // Skip loopback interfaces
         if (!(ifr.ifr_flags & IFF_LOOPBACK)) {
            if (ioctl(sock, SIOCGIFHWADDR, &ifr) == 0) {
               unsigned char *mac = (unsigned char *)ifr.ifr_hwaddr.sa_data;
               snprintf(mac_str, sizeof(mac_str),
                  "%02X%02X%02X%02X%02X%02X",
                  mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
               found = true;
               break;
            }
         }
      }
   }

   close(sock);

   if (!found) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   rbusValue_SetString(value, mac_str);
#endif

   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

static rbusError_t get_system_time(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options) {
   rbusValue_t value;
   rbusValue_Init(&value);

   struct timeval tv;
   if (gettimeofday(&tv, NULL) != 0) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   char time_str[32];
   snprintf(time_str, sizeof(time_str), "%ld.%06ld", (long)tv.tv_sec, (long)tv.tv_usec);

   // Set the rbus value to the formatted time string
   rbusValue_SetString(value, time_str);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

static rbusError_t get_system_uptime(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options) {
   rbusValue_t value;
   rbusValue_Init(&value);

#ifdef __APPLE__
   int mib[2] = {CTL_KERN, KERN_BOOTTIME};
   struct timeval boottime;
   size_t size = sizeof(boottime);

   if (sysctl(mib, 2, &boottime, &size, NULL, 0) == -1) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   struct timeval now;
   if (gettimeofday(&now, NULL) != 0) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   uint32_t uptime_seconds = (now.tv_sec - boottime.tv_sec);
#else
   FILE *fp = fopen("/proc/uptime", "r");
   if (!fp) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   uint32_t uptime_seconds;

   // Read the first value from /proc/uptime (seconds since boot)
   if (fscanf(fp, "%u", &uptime_seconds) != 1) {
      fclose(fp);
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   fclose(fp);
#endif

   // Set the rbus value to the formatted uptime string
   rbusValue_SetUInt32(value, uptime_seconds);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

static rbusError_t get_mac_address(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options) {
   rbusValue_t value;
   rbusValue_Init(&value);

#ifdef __APPLE__
   io_iterator_t iterator;
   kern_return_t kr = IOServiceGetMatchingServices(kIOMainPortDefault,
      IOServiceMatching("IOEthernetInterface"),
      &iterator);
   if (kr != KERN_SUCCESS) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   char mac_str[18] = {0};
   io_service_t service;
   bool found = false;

   while ((service = IOIteratorNext(iterator)) != 0) {
      // Check if this is a non-loopback interface
      CFStringRef bsdName = IORegistryEntryCreateCFProperty(service,
         CFSTR("BSD Name"),
         kCFAllocatorDefault,
         0);
      if (bsdName) {
         char bsd_name[32];
         if (CFStringGetCString(bsdName, bsd_name, sizeof(bsd_name), kCFStringEncodingUTF8)) {
            // Skip loopback (e.g., "lo0")
            if (strncmp(bsd_name, "lo", 2) != 0) {
               // Get MAC address from parent controller
               io_service_t parent;
               kr = IORegistryEntryGetParentEntry(service, kIOServicePlane, &parent);
               if (kr == KERN_SUCCESS) {
                  CFDataRef macData = IORegistryEntryCreateCFProperty(parent,
                     CFSTR("IOMACAddress"),
                     kCFAllocatorDefault,
                     0);
                  if (macData) {
                     const UInt8 *bytes = CFDataGetBytePtr(macData);
                     snprintf(mac_str, sizeof(mac_str),
                        "%02x:%02x:%02x:%02x:%02x:%02x",
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]);
                     CFRelease(macData);
                     found = true;
                  }
                  IOObjectRelease(parent);
               }
            }
         }
         CFRelease(bsdName);
      }
      IOObjectRelease(service);
      if (found) break;
   }
   IOObjectRelease(iterator);

   if (!found) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }
#else
   int sock = socket(AF_INET, SOCK_DGRAM, 0);
   if (sock < 0) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   struct ifreq ifr;
   struct ifconf ifc;
   char buf[1024];
   char mac_str[18] = {0};
   bool found = false;

   ifc.ifc_len = sizeof(buf);
   ifc.ifc_buf = buf;
   if (ioctl(sock, SIOCGIFCONF, &ifc) < 0) {
      close(sock);
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   struct ifreq *it = ifc.ifc_req;
   const struct ifreq *const end = it + (ifc.ifc_len / sizeof(struct ifreq));

   for (; it != end; ++it) {
      strcpy(ifr.ifr_name, it->ifr_name);
      if (ioctl(sock, SIOCGIFFLAGS, &ifr) == 0) {
         // Skip loopback interfaces
         if (!(ifr.ifr_flags & IFF_LOOPBACK)) {
            if (ioctl(sock, SIOCGIFHWADDR, &ifr) == 0) {
               unsigned char *mac = (unsigned char *)ifr.ifr_hwaddr.sa_data;
               snprintf(mac_str, sizeof(mac_str),
                  "%02x:%02x:%02x:%02x:%02x:%02x",
                  mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
               found = true;
               break;
            }
         }
      }
   }

   close(sock);

   if (!found) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }
#endif

   rbusValue_SetString(value, mac_str);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

// Helper function to update memory cache
static bool update_memory_cache(void) {
   time_t now = time(NULL);
   if (g_mem_cache.last_updated + MEMORY_CACHE_TIMEOUT > now) {
      return true;
   }

#ifdef __APPLE__
   int mib[2] = {CTL_HW, HW_MEMSIZE};
   uint64_t total_mem;
   size_t len = sizeof(total_mem);
   if (sysctl(mib, 2, &total_mem, &len, NULL, 0) == -1) {
      return false;
   }

   mach_port_t host_port = mach_host_self();
   vm_size_t page_size;
   vm_statistics64_data_t vm_stat;
   unsigned int count = HOST_VM_INFO64_COUNT;

   if (host_statistics64(host_port, HOST_VM_INFO64, (host_info64_t)&vm_stat, &count) != KERN_SUCCESS ||
      host_page_size(host_port, &page_size) != KERN_SUCCESS) {
      mach_port_deallocate(mach_task_self(), host_port);
      return false;
   }
   mach_port_deallocate(mach_task_self(), host_port);

   g_mem_cache.total = total_mem / 1024;
   g_mem_cache.free = (vm_stat.free_count + vm_stat.inactive_count) * page_size / 1024;
   g_mem_cache.used = (vm_stat.active_count + vm_stat.wire_count) * page_size / 1024;
#else
   FILE *fp = fopen("/proc/meminfo", "r");
   if (!fp) {
      return false;
   }

   char line[256];
   unsigned long mem_total = 0, mem_free = 0, buffers = 0, cached = 0, sreclaimable = 0;
   while (fgets(line, sizeof(line), fp)) {
      if (sscanf(line, "MemTotal: %lu kB", &mem_total) == 1 ||
         sscanf(line, "MemFree: %lu kB", &mem_free) == 1 ||
         sscanf(line, "Buffers: %lu kB", &buffers) == 1 ||
         sscanf(line, "Cached: %lu kB", &cached) == 1 ||
         sscanf(line, "SReclaimable: %lu kB", &sreclaimable) == 1) {
         continue;
      }
   }
   fclose(fp);

   if (mem_total == 0 || mem_free == 0) {
      return false;
   }
   g_mem_cache.total = mem_total;
   g_mem_cache.free = mem_free + buffers + cached + sreclaimable;
   g_mem_cache.used = mem_total - g_mem_cache.free;
#endif

   g_mem_cache.last_updated = now;
   return true;
}

static rbusError_t get_memory_free(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options) {
   rbusValue_t value;
   rbusValue_Init(&value);

   if (!update_memory_cache()) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   rbusValue_SetUInt32(value, (unsigned int)g_mem_cache.free);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);
   return RBUS_ERROR_SUCCESS;
}

static rbusError_t get_memory_used(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options) {
   rbusValue_t value;
   rbusValue_Init(&value);

   if (!update_memory_cache()) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   rbusValue_SetUInt32(value, (unsigned int)g_mem_cache.used);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

static rbusError_t get_memory_total(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options) {
   rbusValue_t value;
   rbusValue_Init(&value);

   if (!update_memory_cache()) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   rbusValue_SetUInt32(value, (unsigned int)g_mem_cache.total);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

static rbusError_t get_local_time(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options) {
   rbusValue_t value;
   rbusValue_Init(&value);

   // Get current time
   time_t rawtime;
   if (time(&rawtime) == (time_t)-1) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   // Convert to local time
   struct tm time_struct;
   struct tm *timeinfo = localtime_r(&rawtime, &time_struct);
   if (!timeinfo) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   // Format time as YYYY-MM-DDThh:mm:ss (e.g., 2024-02-07T23:52:32)
   char time_str[20];
   if (strftime(time_str, sizeof(time_str), "%Y-%m-%dT%H:%M:%S", timeinfo) == 0) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   rbusValue_SetString(value, time_str);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

// Table management
static uint32_t g_nextInstance = 1;
typedef struct {
   char name[MAX_NAME_LEN];
   uint32_t instNum;
   char alias[MAX_NAME_LEN];
} TableRow;

static TableRow *g_tableRows = NULL;
static int g_numTableRows = 0;

static rbusError_t table_add_row(rbusHandle_t handle, const char *tableName, const char *aliasName, uint32_t *instNum) {
   if (!tableName || !instNum) {
      return RBUS_ERROR_INVALID_INPUT;
   }

   g_tableRows = realloc(g_tableRows, (g_numTableRows + 1) * sizeof(TableRow));
   if (!g_tableRows) {
      return RBUS_ERROR_OUT_OF_RESOURCES;
   }

   TableRow *row = &g_tableRows[g_numTableRows];
   snprintf(row->name, MAX_NAME_LEN, "%s.%u", tableName, g_nextInstance);
   row->instNum = g_nextInstance++;
   strncpy(row->alias, aliasName ? aliasName : "", MAX_NAME_LEN - 1);
   row->alias[MAX_NAME_LEN - 1] = '\0';
   *instNum = row->instNum;
   g_numTableRows++;

   rbusEvent_t event = {.name = tableName, .type = RBUS_EVENT_OBJECT_CREATED, .data = NULL};
   rbusError_t rc = rbusEvent_Publish(handle, &event);
   if (rc != RBUS_ERROR_SUCCESS) {
      fprintf(stderr, "Failed to publish table add event for %s: %d\n", tableName, rc);
   }

   return RBUS_ERROR_SUCCESS;
}

static rbusError_t table_remove_row(rbusHandle_t handle, const char *rowName) {
   if (!rowName) {
      return RBUS_ERROR_INVALID_INPUT;
   }

   for (int i = 0; i < g_numTableRows; i++) {
      if (strcmp(g_tableRows[i].name, rowName) == 0 || (g_tableRows[i].alias[0] && strcmp(g_tableRows[i].alias, rowName) == 0)) {
         memmove(&g_tableRows[i], &g_tableRows[i + 1], (g_numTableRows - i - 1) * sizeof(TableRow));
         g_numTableRows--;
         g_tableRows = realloc(g_tableRows, g_numTableRows * sizeof(TableRow));

         char tableName[MAX_NAME_LEN];
         strncpy(tableName, rowName, MAX_NAME_LEN - 1);
         tableName[MAX_NAME_LEN - 1] = '\0';
         char *dot = strrchr(tableName, '.');
         if (dot && dot[1] != '\0') {
            *dot = '\0';
         }

         rbusEvent_t event = {.name = tableName, .type = RBUS_EVENT_OBJECT_DELETED, .data = NULL};
         rbusError_t rc = rbusEvent_Publish(handle, &event);
         if (rc != RBUS_ERROR_SUCCESS) {
            fprintf(stderr, "Failed to publish table remove event for %s: %d\n", tableName, rc);
         }
         return RBUS_ERROR_SUCCESS;
      }
   }
   return RBUS_ERROR_INVALID_INPUT;
}

static rbusError_t system_reboot_method(rbusHandle_t handle, const char *methodName, rbusObject_t inParams, rbusObject_t outParams, rbusMethodAsyncHandle_t asyncHandle) {
   rbusValue_t delayVal = rbusObject_GetValue(inParams, "delay");
   int32_t delay = delayVal ? rbusValue_GetInt32(delayVal) : 0;

   if (delay < 0) {
      rbusObject_SetValue(outParams, "error", rbusValue_InitString("Invalid delay value"));
      return RBUS_ERROR_INVALID_INPUT;
   }

   rbusValue_t resultVal;
   rbusValue_Init(&resultVal);
   rbusValue_SetString(resultVal, "Reboot scheduled");
   rbusObject_SetValue(outParams, "status", resultVal);
   rbusValue_Release(resultVal);

   if (delay > 0) {
      sleep(delay);
   }

   // Simulate reboot (in a real system, this would call system("reboot"))
   printf("System reboot initiated after %d seconds\n", delay);

   return RBUS_ERROR_SUCCESS;
}

static rbusError_t get_system_info_method(rbusHandle_t handle, const char *methodName, rbusObject_t inParams, rbusObject_t outParams, rbusMethodAsyncHandle_t asyncHandle) {

   rbusValue_t serialVal, timeVal, uptimeVal;
   rbusValue_Init(&serialVal);
   rbusValue_Init(&timeVal);
   rbusValue_Init(&uptimeVal);

   rbusProperty_t serialProp = rbusProperty_Init(NULL, "Device.DeviceInfo.SerialNumber", NULL);
   rbusProperty_t timeProp = rbusProperty_Init(NULL, "Device.DeviceInfo.X_RDKCENTRAL-COM_SystemTime", NULL);
   rbusProperty_t uptimeProp = rbusProperty_Init(NULL, "Device.DeviceInfo.UpTime", NULL);

   rbusGetHandlerOptions_t opts = {.context = NULL, .requestingComponent = NULL};
   get_system_serial_number(handle, serialProp, &opts);
   get_system_time(handle, timeProp, &opts);
   get_system_uptime(handle, uptimeProp, &opts);

   rbusObject_SetValue(outParams, "SerialNumber", rbusProperty_GetValue(serialProp));
   rbusObject_SetValue(outParams, "SystemTime", rbusProperty_GetValue(timeProp));
   rbusObject_SetValue(outParams, "UpTime", rbusProperty_GetValue(uptimeProp));

   rbusProperty_Release(serialProp);
   rbusProperty_Release(timeProp);
   rbusProperty_Release(uptimeProp);

   return RBUS_ERROR_SUCCESS;
}

static rbusError_t device_x_rdk_xmidt_send_data(rbusHandle_t handle, const char *methodName, rbusObject_t inParams, rbusObject_t outParams, rbusMethodAsyncHandle_t asyncHandle) {
   // Validate required parameters
   rbusValue_t msgTypeVal = rbusObject_GetValue(inParams, "msg_type");
   rbusValue_t sourceVal = rbusObject_GetValue(inParams, "source");
   rbusValue_t destVal = rbusObject_GetValue(inParams, "dest");

   // Set default msg_type to 4 if not provided
   const char *msg_type_str = "4";
   if (msgTypeVal) {
      if (rbusValue_GetType(msgTypeVal) == RBUS_INT32) {
         if (rbusValue_GetInt32(msgTypeVal) != 4) {
            rbusValue_t errorVal;
            rbusValue_Init(&errorVal);
            rbusValue_SetString(errorVal, "msg_type must be integer 4 or string 'event' (Simple Event)");
            rbusObject_SetValue(outParams, "error", errorVal);
            rbusValue_Release(errorVal);
            return RBUS_ERROR_INVALID_INPUT;
         }
      } else if (rbusValue_GetType(msgTypeVal) == RBUS_STRING) {
         if (strcmp(rbusValue_GetString(msgTypeVal, NULL), "event") != 0) {
            rbusValue_t errorVal;
            rbusValue_Init(&errorVal);
            rbusValue_SetString(errorVal, "msg_type must be integer 4 or string 'event' (Simple Event)");
            rbusObject_SetValue(outParams, "error", errorVal);
            rbusValue_Release(errorVal);
            return RBUS_ERROR_INVALID_INPUT;
         }
         msg_type_str = "event";
      } else {
         rbusValue_t errorVal;
         rbusValue_Init(&errorVal);
         rbusValue_SetString(errorVal, "msg_type must be integer 4 or string 'event' (Simple Event)");
         rbusObject_SetValue(outParams, "error", errorVal);
         rbusValue_Release(errorVal);
         return RBUS_ERROR_INVALID_INPUT;
      }
   }

   if (!sourceVal || rbusValue_GetType(sourceVal) != RBUS_STRING || !rbusValue_GetString(sourceVal, NULL)) {
      rbusValue_t errorVal;
      rbusValue_Init(&errorVal);
      rbusValue_SetString(errorVal, "source must be a non-empty string");
      rbusObject_SetValue(outParams, "error", errorVal);
      rbusValue_Release(errorVal);
      return RBUS_ERROR_INVALID_INPUT;
   }

   if (!destVal || rbusValue_GetType(destVal) != RBUS_STRING || !rbusValue_GetString(destVal, NULL)) {
      rbusValue_t errorVal;
      rbusValue_Init(&errorVal);
      rbusValue_SetString(errorVal, "dest must be a non-empty string");
      rbusObject_SetValue(outParams, "error", errorVal);
      rbusValue_Release(errorVal);
      return RBUS_ERROR_INVALID_INPUT;
   }

   // Extract optional parameters
   const char *source = rbusValue_GetString(sourceVal, NULL);
   const char *dest = rbusValue_GetString(destVal, NULL);
   const char *content_type = NULL;
   rbusValue_t contentTypeVal = rbusObject_GetValue(inParams, "content_type");
   if (contentTypeVal && rbusValue_GetType(contentTypeVal) == RBUS_STRING) {
      content_type = rbusValue_GetString(contentTypeVal, NULL);
   }

   rbusValue_t partnerIdsVal = rbusObject_GetValue(inParams, "partner_ids");
   rbusValue_t headersVal = rbusObject_GetValue(inParams, "headers");
   rbusValue_t metadataVal = rbusObject_GetValue(inParams, "metadata");
   rbusValue_t payloadVal = rbusObject_GetValue(inParams, "payload");
   rbusValue_t sessionIdVal = rbusObject_GetValue(inParams, "session_id");
   rbusValue_t transactionUuidVal = rbusObject_GetValue(inParams, "transaction_uuid");
   rbusValue_t qosVal = rbusObject_GetValue(inParams, "qos");
   rbusValue_t rdrVal = rbusObject_GetValue(inParams, "rdr");

   // Log the event (in a real implementation, this would forward to Xmidt)
   printf("Simple Event Received:\n");
   printf("  Method: %s\n", methodName);
   printf("  msg_type: %s\n", msg_type_str);
   printf("  source: %s\n", source);
   printf("  dest: %s\n", dest);
   if (content_type) printf("  content_type: %s\n", content_type);

   if (partnerIdsVal && rbusValue_GetType(partnerIdsVal) == RBUS_OBJECT) {
      rbusObject_t partnerIdsObj = rbusValue_GetObject(partnerIdsVal);
      if (partnerIdsObj) {
         printf("  partner_ids: [");
         rbusProperty_t prop = rbusObject_GetProperties(partnerIdsObj);
         bool first = true;
         while (prop) {
            rbusValue_t val = rbusProperty_GetValue(prop);
            if (val && rbusValue_GetType(val) == RBUS_STRING) {
               printf("%s%s", first ? "" : ", ", rbusValue_GetString(val, NULL));
               first = false;
            }
            prop = rbusProperty_GetNext(prop);
         }
         printf("]\n");
      }
   }

   if (headersVal && rbusValue_GetType(headersVal) == RBUS_OBJECT) {
      rbusObject_t headersObj = rbusValue_GetObject(headersVal);
      if (headersObj) {
         printf("  headers: [");
         rbusProperty_t prop = rbusObject_GetProperties(headersObj);
         bool first = true;
         while (prop) {
            rbusValue_t val = rbusProperty_GetValue(prop);
            if (val && rbusValue_GetType(val) == RBUS_STRING) {
               printf("%s%s", first ? "" : ", ", rbusValue_GetString(val, NULL));
               first = false;
            }
            prop = rbusProperty_GetNext(prop);
         }
         printf("]\n");
      }
   }

   if (metadataVal && rbusValue_GetType(metadataVal) == RBUS_OBJECT) {
      rbusObject_t metadataObj = rbusValue_GetObject(metadataVal);
      if (metadataObj) {
         printf("  metadata: {");
         rbusProperty_t prop = rbusObject_GetProperties(metadataObj);
         bool first = true;
         while (prop) {
            const char *key = rbusProperty_GetName(prop);
            rbusValue_t val = rbusProperty_GetValue(prop);
            if (key && val && rbusValue_GetType(val) == RBUS_STRING) {
               printf("%s%s: %s", first ? "" : ", ", key, rbusValue_GetString(val, NULL));
               first = false;
            }
            prop = rbusProperty_GetNext(prop);
         }
         printf("}\n");
      }
   }

   if (payloadVal && rbusValue_GetType(payloadVal) == RBUS_STRING) {
      printf("  payload: %s\n", rbusValue_GetString(payloadVal, NULL));
   }
   if (sessionIdVal && rbusValue_GetType(sessionIdVal) == RBUS_STRING) {
      printf("  session_id: %s\n", rbusValue_GetString(sessionIdVal, NULL));
   }
   if (transactionUuidVal && rbusValue_GetType(transactionUuidVal) == RBUS_STRING) {
      printf("  transaction_uuid: %s\n", rbusValue_GetString(transactionUuidVal, NULL));
   }
   if (qosVal && rbusValue_GetType(qosVal) == RBUS_INT32) {
      int32_t qos = rbusValue_GetInt32(qosVal);
      if (qos >= 0 && qos <= 99) {
         printf("  qos: %d\n", qos);
      } else {
         printf("  qos: %d (invalid, must be 0-99)\n", qos);
      }
   }
   if (rdrVal && rbusValue_GetType(rdrVal) == RBUS_INT32) {
      printf("  rdr: %d\n", rbusValue_GetInt32(rdrVal));
   }

   // Set response
   rbusValue_t resultVal;
   rbusValue_Init(&resultVal);
   rbusValue_SetString(resultVal, "Event received");
   rbusObject_SetValue(outParams, "status", resultVal);
   rbusValue_Release(resultVal);

   // Publish an RBUS event to simulate Xmidt event forwarding
   rbusEvent_t event = {.name = dest, .type = RBUS_EVENT_GENERAL, .data = inParams};
   rbusError_t rc = rbusEvent_Publish(handle, &event);
   if (rc != RBUS_ERROR_SUCCESS) {
      fprintf(stderr, "Failed to publish event to %s: %d\n", dest, rc);
   }

   return RBUS_ERROR_SUCCESS;
}


// Data models with new element types
const DataElement gDataElements[] = {
   {
      .name = "Device.DeviceInfo.SerialNumber",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_STRING,
      .value.strVal = "unknown",
      .getHandler = get_system_serial_number,
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
      .name = "Device.Time.CurrentLocalTime",
      .elementType = RBUS_ELEMENT_TYPE_PROPERTY,
      .type = TYPE_DATETIME,
      .value.strVal = "unknown",
      .getHandler = get_local_time,
      .setHandler = NULL,
   },
   {
      .name = "Device.InterfaceTable.",
      .elementType = RBUS_ELEMENT_TYPE_TABLE,
      .type = TYPE_STRING, // Not used for tables, but required for struct compatibility
      .value.strVal = "",
      .tableAddRowHandler = table_add_row,
      .tableRemoveRowHandler = table_remove_row,
      .eventSubHandler = NULL,
   },
   {
      .name = "Device.SystemStatusChanged!",
      .elementType = RBUS_ELEMENT_TYPE_EVENT,
      .type = TYPE_STRING, // Not used for events
      .value.strVal = "",
      .eventSubHandler = NULL,
   },
   {
      .name = "Device.Reboot()",
      .elementType = RBUS_ELEMENT_TYPE_METHOD,
      .type = TYPE_STRING, // Not used for methods
      .value.strVal = "",
      .methodHandler = system_reboot_method,
   },
   {
      .name = "Device.GetSystemInfo()",
      .elementType = RBUS_ELEMENT_TYPE_METHOD,
      .type = TYPE_STRING, // Not used for methods
      .value.strVal = "",
      .methodHandler = get_system_info_method,
   },
   {
      .name = "Device.X_RDK_Xmidt.SendData()",
      .elementType = RBUS_ELEMENT_TYPE_METHOD,
      .type = TYPE_STRING, // Not used for methods
      .value.strVal = "",
      .methodHandler = device_x_rdk_xmidt_send_data,
   }
};

// Callback for handling value change events
void valueChangeHandler(rbusHandle_t handle, rbusEvent_t const *event, rbusEventSubscription_t *subscription) {
   rbusValue_t newValue = rbusObject_GetValue(event->data, "value");
   if (!newValue) {
      printf("Value change event for %s: No new value provided\n", event->name);
      return;
   }

   switch (rbusValue_GetType(newValue)) {
   case RBUS_STRING: {
      char *str = rbusValue_ToString(newValue, NULL, 0);
      printf("Value changed for %s: %s\n", event->name, str);
      free(str);
      break;
   }
   case RBUS_INT32:
      printf("Value changed for %s: %d\n", event->name, rbusValue_GetInt32(newValue));
      break;
   case RBUS_UINT32:
      printf("Value changed for %s: %u\n", event->name, rbusValue_GetUInt32(newValue));
      break;
   case RBUS_BOOLEAN:
      printf("Value changed for %s: %s\n", event->name, rbusValue_GetBoolean(newValue) ? "true" : "false");
      break;
   case RBUS_INT64:
      printf("Value changed for %s: %lld\n", event->name, (long long)rbusValue_GetInt64(newValue));
      break;
   case RBUS_UINT64:
      printf("Value changed for %s: %llu\n", event->name, (unsigned long long)rbusValue_GetUInt64(newValue));
      break;
   case RBUS_SINGLE:
      printf("Value changed for %s: %f\n", event->name, rbusValue_GetSingle(newValue));
      break;
   case RBUS_DOUBLE:
      printf("Value changed for %s: %lf\n", event->name, rbusValue_GetDouble(newValue));
      break;
   case RBUS_BYTE:
      printf("Value changed for %s: %u\n", event->name, rbusValue_GetByte(newValue));
      break;
   default:
      printf("Value changed for %s: Unsupported type\n", event->name);
      break;
   }
}

static rbusError_t eventSubHandler(rbusHandle_t handle, rbusEventSubAction_t action, const char *eventName, rbusFilter_t filter, int32_t interval, bool *autoPublish) {
   printf("Event subscription handler called for %s, action: %s\n", eventName,
      action == RBUS_EVENT_ACTION_SUBSCRIBE ? "subscribe" : "unsubscribe");
   if (autoPublish) {
      *autoPublish = false; // Provider handles event publishing
   }
   return RBUS_ERROR_SUCCESS;
}

bool loadDataElementsFromJson(const char *json_path) {
   FILE *file = fopen(json_path, "r");
   if (!file) {
      fprintf(stderr, "Failed to open JSON file: %s\n", json_path);
      return false;
   }

   fseek(file, 0, SEEK_END);
   long file_size = ftell(file);
   fseek(file, 0, SEEK_SET);
   char *json_str = (char *)malloc(file_size + 1);
   if (!json_str) {
      fprintf(stderr, "Failed to allocate memory for JSON string\n");
      fclose(file);
      return false;
   }
   size_t read_size = fread(json_str, 1, file_size, file);
   json_str[read_size] = '\0';
   fclose(file);

   cJSON *root = cJSON_Parse(json_str);
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

   g_numElements = cJSON_GetArraySize(root);
   if (g_numElements == 0) {
      fprintf(stderr, "No data models found in JSON\n");
      cJSON_Delete(root);
      return false;
   }

   g_totalElements = g_numElements + (sizeof(gDataElements) / sizeof(DataElement));

   // Dynamically allocate memory for g_internalDataElements
   g_internalDataElements = (DataElement *)malloc(g_totalElements * sizeof(DataElement));
   if (!g_internalDataElements) {
      fprintf(stderr, "Failed to allocate memory for data models\n");
      cJSON_Delete(root);
      return false;
   }

   int i = 0;
   for (i = 0; i < g_numElements; i++) {
      cJSON *item = cJSON_GetArrayItem(root, i);
      if (!cJSON_IsObject(item)) {
         fprintf(stderr, "Item %d is not an object\n", i);
         free(g_internalDataElements);
         g_internalDataElements = NULL;
         cJSON_Delete(root);
         return false;
      }

      cJSON *name_obj = cJSON_GetObjectItem(item, "name");
      cJSON *element_type_obj = cJSON_GetObjectItem(item, "elementType");
      cJSON *type_obj = cJSON_GetObjectItem(item, "type");
      cJSON *value_obj = cJSON_GetObjectItem(item, "value");

      if (!cJSON_IsString(name_obj) || !cJSON_IsString(element_type_obj)) {
         fprintf(stderr, "Invalid name or elementType for item %d\n", i);
         free(g_internalDataElements);
         g_internalDataElements = NULL;
         cJSON_Delete(root);
         return false;
      }

      const char *name = cJSON_GetStringValue(name_obj);
      const char *element_type_str = cJSON_GetStringValue(element_type_obj);
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
         free(g_internalDataElements);
         g_internalDataElements = NULL;
         cJSON_Delete(root);
         return false;
      }

      strncpy(g_internalDataElements[i].name, name, MAX_NAME_LEN - 1);
      g_internalDataElements[i].name[MAX_NAME_LEN - 1] = '\0';
      g_internalDataElements[i].elementType = element_type;
      g_internalDataElements[i].getHandler = NULL;
      g_internalDataElements[i].setHandler = NULL;
      g_internalDataElements[i].tableAddRowHandler = NULL;
      g_internalDataElements[i].tableRemoveRowHandler = NULL;
      g_internalDataElements[i].eventSubHandler = NULL;
      g_internalDataElements[i].methodHandler = NULL;

      if (element_type == RBUS_ELEMENT_TYPE_PROPERTY) {
         if (!cJSON_IsNumber(type_obj) || type_obj->valuedouble < 0 || type_obj->valuedouble > TYPE_BYTE) {
            fprintf(stderr, "Invalid type for item %d\n", i);
            free(g_internalDataElements);
            g_internalDataElements = NULL;
            cJSON_Delete(root);
            return false;
         }
         g_internalDataElements[i].type = (ValueType)cJSON_GetNumberValue(type_obj);

         switch (g_internalDataElements[i].type) {
         case TYPE_STRING:
         case TYPE_DATETIME:
         case TYPE_BASE64:
            g_internalDataElements[i].value.strVal = value_obj && cJSON_IsString(value_obj) ? strdup(cJSON_GetStringValue(value_obj)) : strdup("");
            if (!g_internalDataElements[i].value.strVal) {
               fprintf(stderr, "Failed to allocate memory for string value at item %d\n", i);
               free(g_internalDataElements);
               g_internalDataElements = NULL;
               cJSON_Delete(root);
               return false;
            }
            break;
         case TYPE_INT:
            if (value_obj && cJSON_IsNumber(value_obj)) {
               double val = cJSON_GetNumberValue(value_obj);
               if (val >= INT32_MIN && val <= INT32_MAX) {
                  g_internalDataElements[i].value.intVal = (int32_t)val;
               } else {
                  fprintf(stderr, "Value out of range for TYPE_INT at item %d\n", i);
                  free(g_internalDataElements);
                  g_internalDataElements = NULL;
                  cJSON_Delete(root);
                  return false;
               }
            } else {
               g_internalDataElements[i].value.intVal = 0;
            }
            break;
         case TYPE_UINT:
            if (value_obj && cJSON_IsNumber(value_obj)) {
               double val = cJSON_GetNumberValue(value_obj);
               if (val >= 0 && val <= UINT32_MAX) {
                  g_internalDataElements[i].value.uintVal = (uint32_t)val;
               } else {
                  fprintf(stderr, "Value out of range for TYPE_UINT at item %d\n", i);
                  free(g_internalDataElements);
                  g_internalDataElements = NULL;
                  cJSON_Delete(root);
                  return false;
               }
            } else {
               g_internalDataElements[i].value.uintVal = 0;
            }
            break;
         case TYPE_BOOL:
            g_internalDataElements[i].value.boolVal = value_obj && (cJSON_IsTrue(value_obj) || cJSON_IsFalse(value_obj)) ? cJSON_IsTrue(value_obj) : false;
            break;
         case TYPE_LONG:
            if (value_obj && cJSON_IsNumber(value_obj)) {
               double val = cJSON_GetNumberValue(value_obj);
               if (val >= INT64_MIN && val <= INT64_MAX) {
                  g_internalDataElements[i].value.longVal = (int64_t)val;
               } else {
                  fprintf(stderr, "Value out of range for TYPE_LONG at item %d\n", i);
                  free(g_internalDataElements);
                  g_internalDataElements = NULL;
                  cJSON_Delete(root);
                  return false;
               }
            } else {
               g_internalDataElements[i].value.longVal = 0;
            }
            break;
         case TYPE_ULONG:
            if (value_obj && cJSON_IsNumber(value_obj)) {
               double val = cJSON_GetNumberValue(value_obj);
               if (val >= 0 && val <= UINT64_MAX) {
                  g_internalDataElements[i].value.ulongVal = (uint64_t)val;
               } else {
                  fprintf(stderr, "Value out of range for TYPE_ULONG at item %d\n", i);
                  free(g_internalDataElements);
                  g_internalDataElements = NULL;
                  cJSON_Delete(root);
                  return false;
               }
            } else {
               g_internalDataElements[i].value.ulongVal = 0;
            }
            break;
         case TYPE_FLOAT:
            g_internalDataElements[i].value.floatVal = value_obj && cJSON_IsNumber(value_obj) ? (float)cJSON_GetNumberValue(value_obj) : 0.0f;
            break;
         case TYPE_DOUBLE:
            g_internalDataElements[i].value.doubleVal = value_obj && cJSON_IsNumber(value_obj) ? cJSON_GetNumberValue(value_obj) : 0.0;
            break;
         case TYPE_BYTE:
            if (value_obj && cJSON_IsNumber(value_obj)) {
               double val = cJSON_GetNumberValue(value_obj);
               if (val >= 0 && val <= UINT8_MAX) {
                  g_internalDataElements[i].value.byteVal = (uint8_t)val;
               } else {
                  fprintf(stderr, "Value out of range for TYPE_BYTE at item %d\n", i);
                  free(g_internalDataElements);
                  g_internalDataElements = NULL;
                  cJSON_Delete(root);
                  return false;
               }
            } else {
               g_internalDataElements[i].value.byteVal = 0;
            }
            break;
         }
      } else {
         g_internalDataElements[i].type = TYPE_STRING;
         g_internalDataElements[i].value.strVal = strdup("");
         if (!g_internalDataElements[i].value.strVal) {
            fprintf(stderr, "Failed to allocate memory for string value at item %d\n", i);
            free(g_internalDataElements);
            g_internalDataElements = NULL;
            cJSON_Delete(root);
            return false;
         }
      }
   }

   for (int j = 0; i < g_totalElements; i++, j++) {
      strncpy(g_internalDataElements[i].name, gDataElements[j].name, MAX_NAME_LEN - 1);
      g_internalDataElements[i].name[MAX_NAME_LEN - 1] = '\0';
      g_internalDataElements[i].elementType = gDataElements[j].elementType;
      g_internalDataElements[i].type = gDataElements[j].type;
      g_internalDataElements[i].getHandler = gDataElements[j].getHandler;
      g_internalDataElements[i].setHandler = gDataElements[j].setHandler;
      g_internalDataElements[i].tableAddRowHandler = gDataElements[j].tableAddRowHandler;
      g_internalDataElements[i].tableRemoveRowHandler = gDataElements[j].tableRemoveRowHandler;
      g_internalDataElements[i].eventSubHandler = gDataElements[j].eventSubHandler;
      g_internalDataElements[i].methodHandler = gDataElements[j].methodHandler;

      if (gDataElements[j].type == TYPE_STRING || gDataElements[j].type == TYPE_DATETIME || gDataElements[j].type == TYPE_BASE64) {
         g_internalDataElements[i].value.strVal = strdup(gDataElements[j].value.strVal);
         if (!g_internalDataElements[i].value.strVal) {
            fprintf(stderr, "Failed to allocate memory for global data model string\n");
            free(g_internalDataElements);
            g_internalDataElements = NULL;
            cJSON_Delete(root);
            return false;
         }
      } else {
         g_internalDataElements[i].value = gDataElements[j].value;
      }
   }

   cJSON_Delete(root);
   return true;
}

// Callback for handling get requests
rbusError_t getHandler(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t *options) {
   char const *name = rbusProperty_GetName(property);
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
}

rbusError_t setHandler(rbusHandle_t handle, rbusProperty_t property, rbusSetHandlerOptions_t *options) {
   char const *name = rbusProperty_GetName(property);
   rbusValue_t value = rbusProperty_GetValue(property);
   for (int i = 0; i < g_totalElements; i++) {
      if (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY && strcmp(name, g_internalDataElements[i].name) == 0) {
         switch (g_internalDataElements[i].type) {
         case TYPE_STRING:
         case TYPE_DATETIME:
         case TYPE_BASE64: {
            char *str = rbusValue_ToString(value, NULL, 0);
            if (str) {
               free(g_internalDataElements[i].value.strVal);
               g_internalDataElements[i].value.strVal = strdup(str);
               free(str);
               if (!g_internalDataElements[i].value.strVal) {
                  return RBUS_ERROR_OUT_OF_RESOURCES;
               }
            }
            break;
         }
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
         }
         return RBUS_ERROR_SUCCESS;
      }
   }
   return RBUS_ERROR_INVALID_INPUT;
}

static void cleanup(void) {
   if (g_rbusHandle && g_dataElements && g_internalDataElements) {
      rbus_unregDataElements(g_rbusHandle, g_totalElements, g_dataElements);
      for (int i = 0; i < g_totalElements; i++) {
         if (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY ||
            g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_EVENT) {
            rbusEvent_Unsubscribe(g_rbusHandle, g_internalDataElements[i].name);
         }
         if (g_internalDataElements[i].type == TYPE_STRING ||
            g_internalDataElements[i].type == TYPE_DATETIME ||
            g_internalDataElements[i].type == TYPE_BASE64) {
            free(g_internalDataElements[i].value.strVal);
         }
         free(g_dataElements[i].name);
      }
      free(g_dataElements);
      g_dataElements = NULL;
   }
   if (g_tableRows) {
      free(g_tableRows);
      g_tableRows = NULL;
      g_numTableRows = 0;
   }
   if (g_internalDataElements) {
      free(g_internalDataElements);
      g_internalDataElements = NULL;
   }
   if (g_rbusHandle) {
      rbus_close(g_rbusHandle);
      g_rbusHandle = NULL;
   }
}

int main(int argc, char *argv[]) {

   // Set up signal handlers
   signal(SIGINT, signal_handler);
   signal(SIGTERM, signal_handler);

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

   // Dynamically allocate memory for dataElements
   g_dataElements = (rbusDataElement_t *)malloc(g_totalElements * sizeof(rbusDataElement_t));
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
      g_dataElements[i].cbTable.methodHandler = g_internalDataElements[i].methodHandler;
   }

   rc = rbus_regDataElements(g_rbusHandle, g_totalElements, g_dataElements);
   if (rc != RBUS_ERROR_SUCCESS) {
      fprintf(stderr, "Failed to register data elements: %d\n", rc);
      cleanup();
      return 1;
   }

   printf("Successfully registered %d data elements\n", g_totalElements);

   for (int i = 0; i < g_totalElements; i++) {
      if (g_internalDataElements[i].elementType == RBUS_ELEMENT_TYPE_PROPERTY) {
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
         rc = rbus_set(g_rbusHandle, g_internalDataElements[i].name, value, &opts);
         if (rc != RBUS_ERROR_SUCCESS) {
            fprintf(stderr, "Failed to set %s: %d\n", g_internalDataElements[i].name, rc);
         }
         rbusValue_Release(value);
      }
   }

   while (g_running) {
      sleep(1);
   }

   fprintf(stdout, "Shutting down...\n");
   cleanup();
   return 0;
}
