#include "model_alloc.h"

#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static long g_fail_at = LONG_MIN;
static long g_allocation_index = 0;

static bool allocation_should_fail(void) {
   if (g_fail_at == LONG_MIN) {
      const char* configured = getenv("RBUS_ELEMENTS_ALLOC_FAIL_AT");
      if (!configured || !*configured) {
         g_fail_at = -1;
      } else {
         errno = 0;
         char* end = NULL;
         long parsed = strtol(configured, &end, 10);
         g_fail_at = errno == 0 && end && *end == '\0' && parsed >= 0 ? parsed : -1;
      }
   }
   return g_allocation_index++ == g_fail_at;
}

void* model_malloc(size_t size) {
   return allocation_should_fail() ? NULL : malloc(size);
}

void* model_calloc(size_t count, size_t size) {
   return allocation_should_fail() ? NULL : calloc(count, size);
}

void* model_realloc(void* pointer, size_t size) {
   return allocation_should_fail() ? NULL : realloc(pointer, size);
}

char* model_strdup(const char* value) {
   if (!value || allocation_should_fail()) return NULL;
   size_t size = strlen(value) + 1;
   char* copy = malloc(size);
   if (copy) memcpy(copy, value, size);
   return copy;
}