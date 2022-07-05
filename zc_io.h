#ifndef ZC_IO_H
#define ZC_IO_H

#include <stddef.h>
#include <sys/types.h>

typedef struct zc_file zc_file;

zc_file *zc_open(const char *path);
int zc_close(zc_file *file);

const char *zc_read_start(zc_file *file, size_t *size);
void zc_read_end(zc_file *file);

char *zc_write_start(zc_file *file, size_t size);
void zc_write_end(zc_file *file);

off_t zc_lseek(zc_file *file, long offset, int whence);

int zc_copyfile(const char *source, const char *dest);

#endif
