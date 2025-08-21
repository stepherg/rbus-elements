#include "rbus_elements.h"
#include <ifaddrs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>


// Memory cache for optimization
typedef struct {
   uint64_t total;  // Total memory in kB
   uint64_t free;   // Free memory in kB
   uint64_t used;   // Used memory in kB
   time_t last_updated; // Last update timestamp
} MemoryCache;

static MemoryCache g_mem_cache = {0};

// Helper function to update memory cache
static bool update_memory_cache(void) {
   time_t now = time(NULL);
   if (now - g_mem_cache.last_updated < MEMORY_CACHE_TIMEOUT) {
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
   FILE* fp = fopen("/proc/meminfo", "r");
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

rbusError_t get_system_serial_number(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
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

   char* serial = (char*)malloc(maxSize);
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

   struct ifreq* it = ifc.ifc_req;
   const struct ifreq* const end = it + (ifc.ifc_len / sizeof(struct ifreq));

   for (; it != end; ++it) {
      strcpy(ifr.ifr_name, it->ifr_name);
      if (ioctl(sock, SIOCGIFFLAGS, &ifr) == 0) {
         // Skip loopback interfaces
         if (!(ifr.ifr_flags & IFF_LOOPBACK)) {
            if (ioctl(sock, SIOCGIFHWADDR, &ifr) == 0) {
               unsigned char* mac = (unsigned char*)ifr.ifr_hwaddr.sa_data;
               int ret = snprintf(mac_str, sizeof(mac_str),
                  "%02X%02X%02X%02X%02X%02X",
                  mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
               if (ret < 0 || ret >= (int)sizeof(mac_str)) {
                  close(sock);
                  rbusValue_Release(value);
                  return RBUS_ERROR_BUS_ERROR;
               }
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

rbusError_t get_system_time(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
   rbusValue_t value;
   rbusValue_Init(&value);

   struct timeval tv;
   if (gettimeofday(&tv, NULL) != 0) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   char time_str[32];
   int ret = snprintf(time_str, sizeof(time_str), "%ld.%06ld", (long)tv.tv_sec, (long)tv.tv_usec);
   if (ret < 0 || ret >= (int)sizeof(time_str)) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   rbusValue_SetString(value, time_str);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

rbusError_t get_system_uptime(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
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
   FILE* fp = fopen("/proc/uptime", "r");
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

rbusError_t get_mac_address(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
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
         bool bsdNameValid = CFStringGetCString(bsdName, bsd_name, sizeof(bsd_name), kCFStringEncodingUTF8);
         if (bsdNameValid && strncmp(bsd_name, "lo", 2) != 0) {
            io_service_t parent;
            kr = IORegistryEntryGetParentEntry(service, kIOServicePlane, &parent);
            if (kr == KERN_SUCCESS) {
               CFDataRef macData = IORegistryEntryCreateCFProperty(parent,
                  CFSTR("IOMACAddress"),
                  kCFAllocatorDefault,
                  0);
               if (macData) {
                  const UInt8* bytes = CFDataGetBytePtr(macData);
                  int ret = snprintf(mac_str, sizeof(mac_str),
                     "%02x:%02x:%02x:%02x:%02x:%02x",
                     bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]);
                  if (ret < 0 || ret >= (int)sizeof(mac_str)) {
                     CFRelease(macData);
                     IOObjectRelease(parent);
                     CFRelease(bsdName);
                     IOObjectRelease(service);
                     IOObjectRelease(iterator);
                     rbusValue_Release(value);
                     return RBUS_ERROR_BUS_ERROR;
                  }
                  CFRelease(macData);
                  found = true;
                  IOObjectRelease(parent);
               } else {
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

   struct ifreq* it = ifc.ifc_req;
   const struct ifreq* const end = it + (ifc.ifc_len / sizeof(struct ifreq));

   for (; it != end; ++it) {
      strcpy(ifr.ifr_name, it->ifr_name);
      if (ioctl(sock, SIOCGIFFLAGS, &ifr) == 0) {
         // Skip loopback interfaces
         if (!(ifr.ifr_flags & IFF_LOOPBACK)) {
            if (ioctl(sock, SIOCGIFHWADDR, &ifr) == 0) {
               unsigned char* mac = (unsigned char*)ifr.ifr_hwaddr.sa_data;
               int ret = snprintf(mac_str, sizeof(mac_str),
                  "%02x:%02x:%02x:%02x:%02x:%02x",
                  mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
               if (ret < 0 || ret >= (int)sizeof(mac_str)) {
                  close(sock);
                  rbusValue_Release(value);
                  return RBUS_ERROR_BUS_ERROR;
               }
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

rbusError_t get_memory_free(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
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

rbusError_t get_memory_used(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
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

rbusError_t get_memory_total(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
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

rbusError_t get_local_time(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {
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
   struct tm* timeinfo = localtime_r(&rawtime, &time_struct);
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

rbusError_t get_manufacturer_oui(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {

   rbusValue_t value;
   rbusValue_Init(&value);
   char oui_str[18] = {0};

#ifdef __APPLE__
   io_iterator_t iterator;
   kern_return_t kr = IOServiceGetMatchingServices(kIOMainPortDefault,
      IOServiceMatching("IOEthernetInterface"),
      &iterator);
   if (kr != KERN_SUCCESS) {
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

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
         bool bsdNameValid = CFStringGetCString(bsdName, bsd_name, sizeof(bsd_name), kCFStringEncodingUTF8);
         if (bsdNameValid && strncmp(bsd_name, "lo", 2) != 0) {
            io_service_t parent;
            kr = IORegistryEntryGetParentEntry(service, kIOServicePlane, &parent);
            if (kr == KERN_SUCCESS) {
               CFDataRef macData = IORegistryEntryCreateCFProperty(parent,
                  CFSTR("IOMACAddress"),
                  kCFAllocatorDefault,
                  0);
               if (macData) {
                  const UInt8* bytes = CFDataGetBytePtr(macData);
                  int ret = snprintf(oui_str, sizeof(oui_str),
                     "%02X%02X%02X",
                     bytes[0], bytes[1], bytes[2]);
                  if (ret < 0 || ret >= (int)sizeof(oui_str)) {
                     CFRelease(macData);
                     IOObjectRelease(parent);
                     CFRelease(bsdName);
                     IOObjectRelease(service);
                     IOObjectRelease(iterator);
                     rbusValue_Release(value);
                     return RBUS_ERROR_BUS_ERROR;
                  }
                  CFRelease(macData);
                  found = true;
                  IOObjectRelease(parent);
               } else {
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
   bool found = false;

   ifc.ifc_len = sizeof(buf);
   ifc.ifc_buf = buf;
   if (ioctl(sock, SIOCGIFCONF, &ifc) < 0) {
      close(sock);
      rbusValue_Release(value);
      return RBUS_ERROR_BUS_ERROR;
   }

   struct ifreq* it = ifc.ifc_req;
   const struct ifreq* const end = it + (ifc.ifc_len / sizeof(struct ifreq));

   for (; it != end; ++it) {
      strcpy(ifr.ifr_name, it->ifr_name);
      if (ioctl(sock, SIOCGIFFLAGS, &ifr) == 0) {
         // Skip loopback interfaces
         if (!(ifr.ifr_flags & IFF_LOOPBACK)) {
            if (ioctl(sock, SIOCGIFHWADDR, &ifr) == 0) {
               unsigned char* mac = (unsigned char*)ifr.ifr_hwaddr.sa_data;
               int ret = snprintf(oui_str, sizeof(oui_str),
                  "%02X%02X%02X",
                  mac[0], mac[1], mac[2]);
               if (ret < 0 || ret >= (int)sizeof(oui_str)) {
                  close(sock);
                  rbusValue_Release(value);
                  return RBUS_ERROR_BUS_ERROR;
               }
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

   rbusValue_SetString(value, oui_str);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

rbusError_t get_first_ip(rbusHandle_t handle, rbusProperty_t property, rbusGetHandlerOptions_t* options) {

   rbusValue_t value;
   rbusValue_Init(&value);
   char ip_str[18] = {0};

   struct ifaddrs* ifas = NULL, * ifa = NULL;
   int fam_order[2] = {AF_INET, AF_INET6};
   fam_order[0] = AF_INET; fam_order[1] = AF_UNSPEC;

   if (getifaddrs(&ifas) != 0)
      return RBUS_ERROR_BUS_ERROR;

   int rc = -1;
   for (int pass = 0; pass < 2 && rc != 0; ++pass) {
      int want = fam_order[pass];
      if (want == AF_UNSPEC) break;

      for (ifa = ifas; ifa != NULL; ifa = ifa->ifa_next) {
         if (!ifa->ifa_addr) continue;

         // Skip interfaces that are down or are loopback.
         unsigned int flags = ifa->ifa_flags;
         if (!(flags & IFF_UP)) continue;
         if (flags & IFF_LOOPBACK) continue;

         int fam = ifa->ifa_addr->sa_family;
         if (fam != want) continue;

         // Convert to numeric string.
         char host[NI_MAXHOST];
         if (getnameinfo(ifa->ifa_addr,
            (fam == AF_INET) ? sizeof(struct sockaddr_in)
            : sizeof(struct sockaddr_in6),
            host, sizeof(host),
            NULL, 0, NI_NUMERICHOST) != 0) {
            continue;
         }

         // Extra paranoia: exclude 127.0.0.0/8 and ::1 if they sneak through.
         if (fam == AF_INET) {
            struct sockaddr_in* sin = (struct sockaddr_in*)ifa->ifa_addr;
            uint32_t addr = ntohl(sin->sin_addr.s_addr);
            if ((addr >> 24) == 127) continue; // 127.x.x.x
         } else if (fam == AF_INET6) {
            struct sockaddr_in6* sin6 = (struct sockaddr_in6*)ifa->ifa_addr;
            static const struct in6_addr loop6 = IN6ADDR_LOOPBACK_INIT;
            if (memcmp(&sin6->sin6_addr, &loop6, sizeof(loop6)) == 0) continue;
         }

         // Success.
         strncpy(ip_str, host, sizeof(ip_str));
         ip_str[sizeof(ip_str) - 1] = '\0';
         rc = 0;
         break;
      }
   }

   freeifaddrs(ifas);

   rbusValue_SetString(value, ip_str);
   rbusProperty_SetValue(property, value);
   rbusValue_Release(value);

   return RBUS_ERROR_SUCCESS;
}

#if 0
int get_first_non_localhost_ip(char* out, size_t outlen, int allow_v6) {
   if (!out || outlen == 0) return -1;

   struct ifaddrs* ifas = NULL, * ifa = NULL;
   int fam_order[2] = {AF_INET, AF_INET6};
   if (allow_v6) { fam_order[0] = AF_INET; fam_order[1] = AF_INET6; } else { fam_order[0] = AF_INET; fam_order[1] = AF_UNSPEC; }

   if (getifaddrs(&ifas) != 0)
      return -1;

   int rc = -1;

   for (int pass = 0; pass < 2 && rc != 0; ++pass) {
      int want = fam_order[pass];
      if (want == AF_UNSPEC) break;

      for (ifa = ifas; ifa != NULL; ifa = ifa->ifa_next) {
         if (!ifa->ifa_addr) continue;

         // Skip interfaces that are down or are loopback.
         unsigned int flags = ifa->ifa_flags;
         if (!(flags & IFF_UP)) continue;
         if (flags & IFF_LOOPBACK) continue;

         int fam = ifa->ifa_addr->sa_family;
         if (fam != want) continue;

         // Convert to numeric string.
         char host[NI_MAXHOST];
         if (getnameinfo(ifa->ifa_addr,
            (fam == AF_INET) ? sizeof(struct sockaddr_in)
            : sizeof(struct sockaddr_in6),
            host, sizeof(host),
            NULL, 0, NI_NUMERICHOST) != 0) {
            continue;
         }

         // Extra paranoia: exclude 127.0.0.0/8 and ::1 if they sneak through.
         if (fam == AF_INET) {
            struct sockaddr_in* sin = (struct sockaddr_in*)ifa->ifa_addr;
            uint32_t addr = ntohl(sin->sin_addr.s_addr);
            if ((addr >> 24) == 127) continue; // 127.x.x.x
         } else if (fam == AF_INET6) {
            struct sockaddr_in6* sin6 = (struct sockaddr_in6*)ifa->ifa_addr;
            static const struct in6_addr loop6 = IN6ADDR_LOOPBACK_INIT;
            if (memcmp(&sin6->sin6_addr, &loop6, sizeof(loop6)) == 0) continue;
         }

         // Success.
         strncpy(out, host, outlen);
         out[outlen - 1] = '\0';
         rc = 0;
         break;
      }
   }

   freeifaddrs(ifas);
   return rc;
}

#endif