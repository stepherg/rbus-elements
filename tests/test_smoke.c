#include "test_support.h"
#include "test_model_value.h"
#include "test_provider_state.h"
#include "test_psm_store.h"

#include <stdio.h>

static bool test_temporary_directory(void) {
   char path[64];
   TEST_ASSERT(test_make_temp_dir(path, sizeof(path)));
   TEST_ASSERT(test_remove_temp_dir(path));
   return true;
}

#ifndef RBUS_ELEMENTS_TSAN_BUILD
static bool test_rbus_fixture(void) {
   TestRbusFixture fixture;
   TEST_ASSERT(test_rbus_open(&fixture, "smoke"));
   TEST_ASSERT(test_rbus_close(&fixture));
   return true;
}
#endif

int main(void) {
   if (!test_temporary_directory()) {
      return 1;
   }
#ifndef RBUS_ELEMENTS_TSAN_BUILD
   if (!test_rbus_fixture()) return 1;
#endif
   if (!test_model_values()) {
      return 1;
   }
   if (!test_provider_state()) {
      return 1;
   }
   if (!test_psm_store()) {
      return 1;
   }

   puts("rbus-elements test harness ready");
   return 0;
}