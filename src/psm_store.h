#ifndef RBUS_ELEMENTS_PSM_STORE_H
#define RBUS_ELEMENTS_PSM_STORE_H

#include <pthread.h>
#include <rbus/rbus.h>

typedef struct PsmRecord {
   char* name;
   rbusValueType_t type;
   char* value;
   struct PsmRecord* next;
} PsmRecord;

typedef struct {
   pthread_mutex_t mutex;
   PsmRecord* records;
   char* state_path;
} PsmStore;

bool psm_store_init(PsmStore* store, const char* state_path, const char* seed_path,
   char* error, size_t error_size);
void psm_store_destroy(PsmStore* store);
rbusError_t psm_store_set(PsmStore* store, const char* name, rbusValue_t value);
rbusError_t psm_store_get(PsmStore* store, const char* name, rbusValue_t value);

#endif