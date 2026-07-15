#ifndef SERVER_LIST_H
#define SERVER_LIST_H

#include "shared_types.h"
#include <pthread.h>
#include <stddef.h>

#define MAX_SERVERS 128

typedef struct {
    ServerSlot servers[MAX_SERVERS];
    size_t count;
    pthread_mutex_t mutex;
} ServerList;

#endif