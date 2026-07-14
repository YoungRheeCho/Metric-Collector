#ifndef SHARED_TYPES_H
#define SHARED_TYPES_H

#include <stdatomic.h>

#define MAX_SERVERS 128

typedef enum {
    SERVER_STATUS_DOWN = 0,
    SERVER_STATUS_UP   = 1
} ServerStatus;

typedef struct {
    char       ip[16];   /* "255.255.255.255" + NUL */
    int        port;
    atomic_int status;   /* ServerStatus 값, HAProxy가 원자적으로 갱신 */
} ServerSlot;

#endif
