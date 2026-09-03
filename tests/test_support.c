#include "test_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void test_fail(const char* file, int line, const char* expression) {
   fprintf(stderr, "%s:%d: assertion failed: %s\n", file, line, expression);
}

bool test_make_temp_dir(char* path, size_t path_size) {
   static const char template[] = "/tmp/rbus-elements-test-XXXXXX";
   if (!path || path_size < sizeof(template)) {
      return false;
   }

   memcpy(path, template, sizeof(template));
   return mkdtemp(path) != NULL;
}

bool test_remove_temp_dir(const char* path) {
   return path && rmdir(path) == 0;
}

bool test_rbus_open(TestRbusFixture* fixture, const char* test_name) {
   if (!fixture || !test_name) {
      return false;
   }

   int length = snprintf(fixture->component_name, sizeof(fixture->component_name),
      "rbus-elements-test-%ld-%s", (long)getpid(), test_name);
   if (length < 0 || (size_t)length >= sizeof(fixture->component_name)) {
      return false;
   }

   fixture->handle = NULL;
   return rbus_open(&fixture->handle, fixture->component_name) == RBUS_ERROR_SUCCESS;
}

bool test_rbus_close(TestRbusFixture* fixture) {
   if (!fixture || !fixture->handle) {
      return false;
   }

   rbusError_t error = rbus_close(fixture->handle);
   fixture->handle = NULL;
   return error == RBUS_ERROR_SUCCESS;
}