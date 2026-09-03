#ifndef RBUS_ELEMENTS_MODEL_ALLOC_H
#define RBUS_ELEMENTS_MODEL_ALLOC_H

#include <stddef.h>

void* model_malloc(size_t size);
void* model_calloc(size_t count, size_t size);
void* model_realloc(void* pointer, size_t size);
char* model_strdup(const char* value);

#endif