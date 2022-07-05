#define _GNU_SOURCE 1
#include "zc_io.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

// The zc_file struct is analogous to the FILE struct that you get from fopen.
struct zc_file
{
  void *base_ptr;
  int fd;
  off_t size;
  off_t offset;

  sem_t *mutex;
  sem_t *room_empty;
  int n_readers;
};

zc_file *zc_open(const char *path)
{
  int fd = open(path, O_CREAT | O_RDWR, 0666);
  if (fd == -1)
  {
    perror("Error while opening file");
    return NULL;
  }

  struct stat stat_buffer;
  if (fstat(fd, &stat_buffer) == -1)
  {
    perror("Error with fstat");
    return NULL;
  }

  off_t file_size;
  if (stat_buffer.st_size == 0)
  {
    file_size = 1; // allows for valid mmap subsequently
  }
  else
  {
    file_size = stat_buffer.st_size;
  }

  void *map_ptr = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (map_ptr == MAP_FAILED)
  {
    perror("Error while mapping");
    return NULL;
  }

  sem_t *mutex = malloc(sizeof(sem_t));
  sem_init(mutex, 1, 1);
  sem_t *room_empty = malloc(sizeof(sem_t));
  sem_init(room_empty, 1, 1);

  zc_file *file = (zc_file *)malloc(sizeof(zc_file));
  file->base_ptr = map_ptr;
  file->fd = fd;
  file->size = file_size;
  file->offset = 0;
  file->mutex = mutex;
  file->room_empty = room_empty;
  file->n_readers = 0;
  return file;
}

int zc_close(zc_file *file)
{
  if (fsync(file->fd) == -1)
  {
    return -1;
  }
  if (munmap(file->base_ptr, file->size) == -1)
  {
    perror("Error while unmapping");
    return -1;
  }
  if (close(file->fd) == -1)
  {
    perror("Error while closing fd");
    return -1;
  }
  sem_destroy(file->mutex);
  sem_destroy(file->room_empty);
  free(file);
  return 0;
}

const char *zc_read_start(zc_file *file, size_t *size)
{
  sem_t *mutex = file->mutex;
  sem_t *room_empty = file->room_empty;
  sem_wait(mutex);
  file->n_readers++;
  if (file->n_readers == 1)
  {
    sem_wait(room_empty);
  }
  sem_post(mutex);

  // offset has reached (beyond) EOF
  if (file->offset >= file->size)
  {
    *size = 0;
    return NULL;
  }

  long remaining_size = file->size - file->offset;
  if ((long)*size > remaining_size)
  {
    *size = (size_t)remaining_size;
  }

  void *file_ptr = file->base_ptr + file->offset;
  file->offset += *size;

  return file_ptr;
}

void zc_read_end(zc_file *file)
{
  sem_t *mutex = file->mutex;
  sem_t *room_empty = file->room_empty;
  sem_wait(mutex);
  file->n_readers--;
  if (file->n_readers == 0)
  {
    sem_post(room_empty);
  }
  sem_post(mutex);
}

char *zc_write_start(zc_file *file, size_t size)
{
  sem_wait(file->room_empty);
  long remaining_size = file->size - file->offset;

  if ((long)size > remaining_size) // this also accounts for after a lseek call to beyond SEEK_END, remaining_size will be <= 0
  {
    off_t new_size = file->offset + size;
    if (ftruncate(file->fd, new_size) == -1)
    {
      perror("Error with truncating\n");
      return NULL;
    }

    void *new_base_ptr = mremap(file->base_ptr, file->size, new_size, MREMAP_MAYMOVE);
    if (new_base_ptr == MAP_FAILED)
    {
      perror("Error while remapping");
      return NULL;
    }

    memset((char *)new_base_ptr + file->size, '\0', new_size - file->size); // set all nearly expanded positions to null byte

    file->base_ptr = new_base_ptr;
    file->size = new_size;
  }

  void *file_ptr = file->base_ptr + file->offset;
  file->offset += size;

  return file_ptr;
}

void zc_write_end(zc_file *file)
{
  if (msync(file->base_ptr, file->size, MS_SYNC) == -1)
  {
    perror("Error while flushing after write");
  }
  sem_post(file->room_empty);
}

off_t zc_lseek(zc_file *file, long offset, int whence)
{
  sem_wait(file->room_empty);
  off_t new_offset;
  if (whence == SEEK_SET)
  {
    new_offset = offset;
  }
  else if (whence == SEEK_CUR)
  {
    new_offset = file->offset + offset;
  }
  else if (whence == SEEK_END)
  {
    new_offset = file->size + offset; // SEEK_END starts from one position past the last byte of the file
  }
  else
  {
    new_offset = (off_t)-1;
  }

  // when new_offset is set to before the start of the file
  if (new_offset < 0)
  {
    return -1;
  }

  file->offset = new_offset;
  sem_post(file->room_empty);
  return new_offset;
}

int zc_copyfile(const char *source, const char *dest)
{
  int source_fd = open(source, O_CREAT | O_RDWR, 0666);
  if (source_fd == -1)
  {
    perror("Error while opening source file");
    return -1;
  }

  struct stat stat_buffer;
  if (fstat(source_fd, &stat_buffer) == -1)
  {
    perror("Error with fstat");
    return -1;
  }

  int dest_fd = open(dest, O_CREAT | O_RDWR, 0666);
  if (dest_fd == -1)
  {
    perror("Error while opening source file");
    return -1;
  }
  if (ftruncate(dest_fd, stat_buffer.st_size) == -1)
  {
    perror("Error with truncating dest file\n");
    return -1;
  }

  if (copy_file_range(source_fd, NULL, dest_fd, NULL, stat_buffer.st_size, 0) == -1)
  {
    perror("Error while copying file");
    return -1;
  }
  if (close(dest_fd) == -1)
  {
    perror("Error while closing dest_fd");
    return -1;
  }

  return 0;
}
