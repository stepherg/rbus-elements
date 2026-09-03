#include "test_psm_store.h"

#include "psm_store.h"
#include "test_support.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define RFC_NAME "eRT.com.cisco.spvtg.ccsp.webpa.WebConfigRfcEnable"

static char* read_text(const char* path) {
   FILE* file = fopen(path, "rb");
   if (!file) return NULL;
   if (fseek(file, 0, SEEK_END) != 0) {
      fclose(file);
      return NULL;
   }
   long length = ftell(file);
   if (length < 0 || fseek(file, 0, SEEK_SET) != 0) {
      fclose(file);
      return NULL;
   }
   char* text = malloc((size_t)length + 1);
   if (text && fread(text, 1, (size_t)length, file) == (size_t)length) {
      text[length] = '\0';
   } else {
      free(text);
      text = NULL;
   }
   fclose(file);
   return text;
}

bool test_psm_store(void) {
   char directory[64];
   TEST_ASSERT(test_make_temp_dir(directory, sizeof(directory)));
   char state_path[128];
   snprintf(state_path, sizeof(state_path), "%s/psm.json", directory);
   char error[256];
   PsmStore store;
   TEST_ASSERT(psm_store_init(&store, state_path, RBUS_ELEMENTS_TEST_SEED, error, sizeof(error)));

   rbusValue_t value;
   rbusValue_Init(&value);
   TEST_ASSERT(psm_store_get(&store, RFC_NAME, value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_STRING);
   TEST_ASSERT(strcmp(rbusValue_GetString(value, NULL), "true") == 0);
   rbusValue_SetBoolean(value, false);
   TEST_ASSERT(psm_store_set(&store, RFC_NAME, value) == RBUS_ERROR_SUCCESS);
   rbusValue_SetInt32(value, 100);
   TEST_ASSERT(psm_store_set(&store, "Device.Test.Integer", value) == RBUS_ERROR_SUCCESS);
   psm_store_destroy(&store);

   TEST_ASSERT(psm_store_init(&store, state_path, RBUS_ELEMENTS_TEST_SEED, error, sizeof(error)));
   TEST_ASSERT(psm_store_get(&store, RFC_NAME, value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_BOOLEAN && !rbusValue_GetBoolean(value));
   TEST_ASSERT(psm_store_get(&store, "Device.Test.Integer", value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_INT32 && rbusValue_GetInt32(value) == 100);

   char* before = read_text(state_path);
   TEST_ASSERT(before != NULL);
   setenv("RBUS_ELEMENTS_PSM_FAIL_WRITE", "1", 1);
   rbusValue_SetBoolean(value, true);
   TEST_ASSERT(psm_store_set(&store, RFC_NAME, value) == RBUS_ERROR_BUS_ERROR);
   unsetenv("RBUS_ELEMENTS_PSM_FAIL_WRITE");
   TEST_ASSERT(psm_store_get(&store, RFC_NAME, value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_BOOLEAN && !rbusValue_GetBoolean(value));
   char* after = read_text(state_path);
   TEST_ASSERT(after != NULL && strcmp(before, after) == 0);
   free(before);
   free(after);

   rbusValue_Release(value);
   psm_store_destroy(&store);
   TEST_ASSERT(unlink(state_path) == 0);
   TEST_ASSERT(test_remove_temp_dir(directory));
   return true;
}
