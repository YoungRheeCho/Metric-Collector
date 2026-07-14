#ifndef CHANNEL_SHM_H
#define CHANNEL_SHM_H

#include "channel.h"
#include <stddef.h>

Channel* shm_channel_create(const char* shm_name, size_t element_size, size_t max_elements, int create, int writable);

#endif
