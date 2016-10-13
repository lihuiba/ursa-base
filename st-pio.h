#ifndef __ST_PIO__
#define __ST_PIO__

#include <stddef.h>

struct iovec;

int st_pio_init();

ssize_t st_pread(int fd, void* buf, size_t count, off_t offset);
ssize_t st_pwrite(int fd, const void *buf, size_t count, off_t offset);
ssize_t st_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset);
ssize_t st_pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset);
int st_fsync(int fd);

void st_pio_fini();



#endif
