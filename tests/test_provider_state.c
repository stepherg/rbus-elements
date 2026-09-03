#include "test_provider_state.h"

#include "provider_state.h"
#include "test_support.h"

#include <pthread.h>

TableDef* g_tables = NULL;
int g_num_tables = 0;

typedef struct {
   DataElement* element;
   bool failed;
} StateThread;

typedef struct {
   const char* table_name;
   int additions;
   bool failed;
} TableThread;

static void reset_tables(void) {
   pthread_mutex_lock(&g_provider_mutex);
   for (int i = 0; i < g_num_tables; i++) {
      for (int j = 0; j < g_tables[i].num_rows; j++) provider_row_free(&g_tables[i].rows[j]);
      free(g_tables[i].rows);
   }
   free(g_tables);
   g_tables = NULL;
   g_num_tables = 0;
   pthread_mutex_unlock(&g_provider_mutex);
}

static void* write_strings(void* context) {
   StateThread* thread = context;
   for (int i = 0; i < 20000; i++) {
      char text[64];
      snprintf(text, sizeof(text), "complete-value-%d-complete", i);
      ElementValue replacement = {.strVal = strdup(text)};
      if (!replacement.strVal) {
         thread->failed = true;
         return NULL;
      }
      provider_property_replace(thread->element, &replacement);
   }
   return NULL;
}

static void* read_strings(void* context) {
   StateThread* thread = context;
   for (int i = 0; i < 20000; i++) {
      ElementValue snapshot;
      if (!provider_property_snapshot(thread->element, &snapshot)) {
         thread->failed = true;
         return NULL;
      }
      size_t length = strlen(snapshot.strVal);
      if (strncmp(snapshot.strVal, "complete-value-", 15) != 0 || length < 24 ||
         strcmp(snapshot.strVal + length - 9, "-complete") != 0) {
         thread->failed = true;
      }
      free(snapshot.strVal);
      if (thread->failed) return NULL;
   }
   return NULL;
}

static void* add_rows(void* context) {
   TableThread* thread = context;
   for (int i = 0; i < thread->additions; i++) {
      uint32_t instance;
      if (provider_table_add(thread->table_name, NULL, &instance) != RBUS_ERROR_SUCCESS) {
         thread->failed = true;
         return NULL;
      }
   }
   return NULL;
}

static void* read_row_until_removed(void* context) {
   TableThread* thread = context;
   for (int i = 0; i < 10000; i++) {
      ElementValue snapshot;
      rbusError_t error = provider_row_snapshot(thread->table_name, 1, "Value", TYPE_STRING, &snapshot);
      if (error == RBUS_ERROR_SUCCESS) {
         free(snapshot.strVal);
      } else if (error != RBUS_ERROR_BUS_ERROR) {
         thread->failed = true;
         return NULL;
      }
   }
   return NULL;
}

static bool test_row_validation(void) {
   uint32_t instance;
   TEST_ASSERT(provider_table_add("Device.Test.Table.", "first", &instance) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(instance == 1);
   rbusValue_t invalid;
   rbusValue_Init(&invalid);
   rbusValue_SetString(invalid, "not-an-integer");
   TEST_ASSERT(provider_row_set("Device.Test.Table.", instance, "Value", TYPE_UINT, invalid) == RBUS_ERROR_INVALID_INPUT);
   TEST_ASSERT(!provider_row_property_exists("Device.Test.Table.", instance, "Value"));
   rbusValue_Release(invalid);

   ElementValue snapshot;
   TEST_ASSERT(provider_row_snapshot("Device.Test.Table.", instance, "Value", TYPE_UINT, &snapshot) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(snapshot.uintVal == 0);
   TEST_ASSERT(provider_table_add("Device.Test.Table.", "first", &instance) == RBUS_ERROR_ELEMENT_NAME_DUPLICATE);
   uint32_t count;
   TEST_ASSERT(provider_table_count("Device.Test.Table.", &count) && count == 1);
   reset_tables();
   return true;
}

static bool test_instance_exhaustion(void) {
   uint32_t instance;
   TEST_ASSERT(provider_table_add("Device.Test.Exhaust.", NULL, &instance) == RBUS_ERROR_SUCCESS);
   pthread_mutex_lock(&g_provider_mutex);
   g_tables[0].next_inst = UINT32_MAX;
   pthread_mutex_unlock(&g_provider_mutex);
   TEST_ASSERT(provider_table_add("Device.Test.Exhaust.", NULL, &instance) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(instance == UINT32_MAX);
   TEST_ASSERT(provider_table_add("Device.Test.Exhaust.", NULL, &instance) == RBUS_ERROR_OUT_OF_RESOURCES);
   uint32_t count;
   TEST_ASSERT(provider_table_count("Device.Test.Exhaust.", &count) && count == 2);
   reset_tables();
   return true;
}

static bool test_sparse_registered_rows(void) {
   TEST_ASSERT(provider_table_commit_registered("Device.Test.Sparse.", 1, NULL) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(provider_table_commit_registered("Device.Test.Sparse.", 10, NULL) == RBUS_ERROR_SUCCESS);
   uint32_t count;
   TEST_ASSERT(provider_table_count("Device.Test.Sparse.", &count) && count == 2);
   TEST_ASSERT(provider_table_commit_registered("Device.Test.Sparse.", 10, NULL) == RBUS_ERROR_ELEMENT_NAME_DUPLICATE);
   TEST_ASSERT(provider_table_count("Device.Test.Sparse.", &count) && count == 2);
   pthread_mutex_lock(&g_provider_mutex);
   TEST_ASSERT(g_tables[0].rows[0].instNum == 1);
   TEST_ASSERT(g_tables[0].rows[1].instNum == 10);
   pthread_mutex_unlock(&g_provider_mutex);
   reset_tables();
   return true;
}

static bool test_concurrent_tables(void) {
   TableThread first = {.table_name = "Device.Test.Concurrent.", .additions = 1000};
   TableThread second = {.table_name = "Device.Test.Concurrent.", .additions = 1000};
   pthread_t first_thread;
   pthread_t second_thread;
   TEST_ASSERT(pthread_create(&first_thread, NULL, add_rows, &first) == 0);
   TEST_ASSERT(pthread_create(&second_thread, NULL, add_rows, &second) == 0);
   TEST_ASSERT(pthread_join(first_thread, NULL) == 0);
   TEST_ASSERT(pthread_join(second_thread, NULL) == 0);
   TEST_ASSERT(!first.failed && !second.failed);
   uint32_t count;
   TEST_ASSERT(provider_table_count(first.table_name, &count) && count == 2000);
   reset_tables();

   uint32_t instance;
   TEST_ASSERT(provider_table_add("Device.Test.Remove.", NULL, &instance) == RBUS_ERROR_SUCCESS);
   TableThread reader = {.table_name = "Device.Test.Remove."};
   pthread_t reader_thread;
   TEST_ASSERT(pthread_create(&reader_thread, NULL, read_row_until_removed, &reader) == 0);
   TableRow removed = {0};
   TEST_ASSERT(provider_table_remove(reader.table_name, instance, NULL, &removed) == RBUS_ERROR_SUCCESS);
   provider_row_free(&removed);
   TEST_ASSERT(pthread_join(reader_thread, NULL) == 0);
   TEST_ASSERT(!reader.failed);
   TEST_ASSERT(provider_table_count(reader.table_name, &count) && count == 0);
   reset_tables();
   return true;
}

bool test_provider_state(void) {
   DataElement element = {.type = TYPE_STRING, .value.strVal = strdup("complete-value-0-complete")};
   TEST_ASSERT(element.value.strVal != NULL);
   StateThread thread = {.element = &element, .failed = false};
   pthread_t writer;
   pthread_t reader;
   TEST_ASSERT(pthread_create(&writer, NULL, write_strings, &thread) == 0);
   TEST_ASSERT(pthread_create(&reader, NULL, read_strings, &thread) == 0);
   TEST_ASSERT(pthread_join(writer, NULL) == 0);
   TEST_ASSERT(pthread_join(reader, NULL) == 0);
   TEST_ASSERT(!thread.failed);
   free(element.value.strVal);
   TEST_ASSERT(test_row_validation());
   TEST_ASSERT(test_instance_exhaustion());
   TEST_ASSERT(test_sparse_registered_rows());
   TEST_ASSERT(test_concurrent_tables());
   return true;
}