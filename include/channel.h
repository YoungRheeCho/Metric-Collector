#ifndef CHANNEL_H
#define CHANNEL_H

#include <stddef.h>

typedef struct Channel Channel;

struct Channel {
    const char *channel_id;
    int (*init)(Channel *self);
    ssize_t (*read)(Channel *self, void *buf, size_t max_count);
    int (*write)(Channel *self, const void *data, size_t count);
    void (*close)(Channel *self);
    void *impl_data;
};

#endif
