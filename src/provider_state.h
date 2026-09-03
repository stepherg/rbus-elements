#ifndef RBUS_ELEMENTS_PROVIDER_STATE_H
#define RBUS_ELEMENTS_PROVIDER_STATE_H

#include "rbus_elements.h"

#include <pthread.h>

extern pthread_mutex_t g_provider_mutex;

bool provider_value_type_matches(ValueType type, rbusValue_t value);
bool provider_value_from_rbus(ValueType type, rbusValue_t source, ElementValue* destination);
void provider_value_to_rbus(ValueType type, const ElementValue* source, rbusValue_t destination);
bool provider_property_snapshot(const DataElement* element, ElementValue* snapshot);
void provider_property_replace(DataElement* element, ElementValue* replacement);
bool provider_table_count(const char* table_name, uint32_t* count);
rbusError_t provider_table_add(const char* table_name, const char* alias, uint32_t* instance);
rbusError_t provider_table_commit_registered(const char* table_name, uint32_t instance, const char* alias);
rbusError_t provider_table_remove(const char* table_name, uint32_t instance,
	const char* alias, TableRow* removed);
rbusError_t provider_row_snapshot(const char* table_name, uint32_t instance,
	const char* property_name, ValueType type, ElementValue* snapshot);
rbusError_t provider_row_set(const char* table_name, uint32_t instance,
	const char* property_name, ValueType type, rbusValue_t value);
bool provider_row_property_exists(const char* table_name, uint32_t instance,
	const char* property_name);
void provider_row_free(TableRow* row);

#endif