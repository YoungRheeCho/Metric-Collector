#include "channel_shm.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
    char shm_name[64];   // shm 이름
    size_t element_size; // channel이 다루는 데이터의 크기 (shared memory의
                         // total size 계산에 이용)
    size_t max_elements; // 데이터의 최대 개수 (shared memory의 total size
                         // 계산에 이용)
    int create;          // shm open할 떄, create 할지말지
    int writable;        // shm에 쓰기 권한
    int fd;              // shm의 file descriptor
    void *mapped;        // mmap return 값
} ShmChannelState;

static int shm_channel_init(Channel *self) {
    ShmChannelState *st = (ShmChannelState *)self->impl_data;
    size_t total_size = st->element_size * st->max_elements;

    int flags =
        st->create ? (O_CREAT | O_RDWR) : (st->writable ? O_RDWR : O_RDONLY);
    st->fd = shm_open(st->shm_name, flags, 0666);
    if (st->fd == -1) {
        perror("shm_open");
        return -1;
    }

    // create했을 경우 shm 메모리 크기 지정
    if (st->create) {
        if (ftruncate(st->fd, (off_t)total_size) == -1) {
            perror("ftruncate");
            close(st->fd);
            st->fd = -1;
            return -1;
        }
    }

    int prot = st->writable ? (PROT_READ | PROT_WRITE) : PROT_READ;
    st->mapped = mmap(NULL, total_size, prot, MAP_SHARED, st->fd, 0);
    if (st->mapped == MAP_FAILED) {
        perror("mmap");
        close(st->fd);
        st->fd = -1;
        st->mapped = NULL;
        return -1;
    }

    return 0;
}

static ssize_t shm_channel_read(Channel *self, void *buf, size_t max_count) {
    ShmChannelState *st = (ShmChannelState *)self->impl_data;
    if (!st->mapped)
        return -1;

    size_t n = max_count < st->max_elements ? max_count : st->max_elements;
    memcpy(buf, st->mapped, n * st->element_size);
    return n;
}

static int shm_channel_write(Channel *self, const void *data, size_t count) {
    ShmChannelState *st = (ShmChannelState *)self->impl_data;
    if (!st->writable || !st->mapped)
        return -1;

    size_t n = count < st->max_elements ? count : st->max_elements;
    memcpy(st->mapped, data, n * st->element_size);
    return 0;
}

static void shm_channel_close(Channel *self) {
    ShmChannelState *st = (ShmChannelState *)self->impl_data;
    if (st->mapped) {
        munmap(st->mapped, st->element_size * st->max_elements);
    }
    if (st->fd != -1) {
        close(st->fd);
    }
    free(st);
    free(self);
}

// 생성자
Channel *shm_channel_create(const char *shm_name, size_t element_size,
                            size_t max_elements, int create, int writable) {
    ShmChannelState *st = malloc(sizeof(ShmChannelState));
    if (!st)
        return NULL;
    memset(st, 0, sizeof(*st));

    strncpy(st->shm_name, shm_name, sizeof(st->shm_name) - 1);
    st->element_size = element_size;
    st->max_elements = max_elements;
    st->create = create;
    st->writable = writable;
    st->fd = -1;
    st->mapped = NULL;

    Channel *ch = malloc(sizeof(Channel));
    if (!ch) {
        free(st);
        return NULL;
    }
    ch->channel_id = st->shm_name;
    ch->init = shm_channel_init;
    ch->read = shm_channel_read;
    ch->write = shm_channel_write;
    ch->close = shm_channel_close;
    ch->impl_data = st;
    return ch;
}
