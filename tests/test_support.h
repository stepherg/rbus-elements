#ifndef RBUS_ELEMENTS_TEST_SUPPORT_H
#define RBUS_ELEMENTS_TEST_SUPPORT_H

#include <stdbool.h>
#include <stddef.h>
#include <rbus/rbus.h>

typedef struct {
   rbusHandle_t handle;
   char component_name[96];
} TestRbusFixture;

#define TEST_ASSERT(condition) \
   do { \
      if (!(condition)) { \
         test_fail(__FILE__, __LINE__, #condition); \
         return false; \
      } \
   } while (0)

void test_fail(const char* file, int line, const char* expression);
bool test_make_temp_dir(char* path, size_t path_size);
bool test_remove_temp_dir(const char* path);
bool test_rbus_open(TestRbusFixture* fixture, const char* test_name);
bool test_rbus_close(TestRbusFixture* fixture);

#endif