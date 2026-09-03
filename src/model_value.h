#ifndef RBUS_ELEMENTS_MODEL_VALUE_H
#define RBUS_ELEMENTS_MODEL_VALUE_H

#include "rbus_elements.h"

bool model_value_parse(const cJSON* json, ValueType type, ElementValue* value,
   char* error, size_t error_size);
void model_value_free(ValueType type, ElementValue* value);

#endif