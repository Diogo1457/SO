#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <math.h>
#include <fcntl.h>
#include <pthread.h>

#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;

int write_all(int fd, const char *buf, size_t len) {
    size_t total_written = 0;
    while (total_written < len) {
        ssize_t bytes_written = write(fd, buf + total_written, len - total_written);
        if (bytes_written < 0) {
            return -1;
        }
        total_written += (size_t)bytes_written;
    }
    
    if (total_written != len) {
        return -1;
    }
    
    return 0;
}

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table(TABLE_SIZE);
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  free_table(kvs_table);
  kvs_table = NULL;
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

/// Sort a 2D array of strings in ascending order using a simple bubble sort algorithm.
/// @param keys A 2D array of strings to be sorted.
/// @param rows The number of rows (strings) in the array.
/// @param cols The maximum size of each string (column size).
void sortArray(char keys[][MAX_STRING_SIZE], int rows, int cols) {
  char temp[cols];
  for (int i = 0; i < rows - 1; i++) {
    for (int j = i + 1; j < rows; j++) {
      if (strcmp(keys[i], keys[j]) > 0) {
        strcpy(temp, keys[i]);
        strcpy(keys[i], keys[j]);
        strcpy(keys[j], temp);
      }
    }
  }
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd_out) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  sortArray(keys, (int)num_pairs, MAX_STRING_SIZE);
  if (write_all(fd_out, "[", 1) < 0) return 1;
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      size_t buffer_size = strlen(keys[i])+12;
      char* buffer = (char*) malloc(sizeof(char)*buffer_size);
      snprintf(buffer, buffer_size, "(%s,KVSERROR)", keys[i]);
      if (write_all(fd_out, buffer, buffer_size-1) < 0) {
        free(buffer);
        return 1;
      }
      free(buffer);
    } else {
      size_t buffer_size = strlen(keys[i])+strlen(result)+4;
      char* buffer = (char*) malloc(sizeof(char)*buffer_size);
      snprintf(buffer, buffer_size, "(%s,%s)", keys[i], result);
      if (write_all(fd_out, buffer, buffer_size-1) < 0) {
        free(buffer);
        free(result);
        return 1;
      }
      free(buffer);
    }
    free(result);
  }
  if (write_all(fd_out, "]\n", 2) < 0) return 1;
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd_out) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        if (write_all(fd_out, "[", 1) < 0) return 1;
        aux = 1;
      }
      size_t buffer_size = (strlen(keys[i]) + 14);
      char* buffer = (char*) malloc(sizeof(char)*buffer_size);
      snprintf(buffer, buffer_size, "(%s,KVSMISSING)", keys[i]);
      if (write_all(fd_out, buffer, buffer_size-1) < 0) {
        free(buffer);
        return 1;
      }
      free(buffer);
    }
  }
  if (aux) {
    if (write_all(fd_out, "]\n", 2) < 0) return 1;
  }
  return 0;
}

void kvs_show(int fd_out) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      size_t buffer_size = strlen(keyNode->key) + strlen(keyNode->value) + 6;
      char *buffer = (char*) malloc(sizeof(char)*(buffer_size));
      snprintf(buffer, buffer_size, "(%s, %s)\n", keyNode->key, keyNode->value); 
      if (write_all(fd_out, buffer, buffer_size-1) < 0) {
        free(buffer);
        return;
      }
      free(buffer);
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

int isExtensionFile(const char *filename, const char* extension) {
  size_t len_filename = strlen(filename);
  size_t len_extension = strlen(extension);
  if (len_filename >= len_extension) {
    return strcmp(filename + len_filename - len_extension, extension) == 0;
  }
  return 0;
}

/// Remove the file extension from a filepath.
/// @param filepath The filepath to remove the extension.
void removeExtension(char *filepath) {
  char *last_dot = strrchr(filepath, '.');
  if (last_dot != NULL && strchr(last_dot, '/') == NULL) {
    *last_dot = '\0';
  }
}

char* changeFileExtension(char* filepath, const char* extension) {
  char* output_filepath = NULL;
  size_t output_filepath_size = strlen(filepath) + 1;
  output_filepath = (char*) malloc(output_filepath_size);
  if (!output_filepath) {
    return NULL;
  }
  strncpy(output_filepath, filepath, output_filepath_size);
  removeExtension(output_filepath);
  strncat(output_filepath, extension, output_filepath_size - strlen(output_filepath) - 1);
  return output_filepath;
}

/// Calculate the number of digits in a positive integer.
/// @param number The integer whose length (number of digits) is to be determined.
/// @return The number of digits in the integer. Returns 1 for 0 (as it has one digit).
long unsigned int getLengthOfNumber(int number) {
  if (number == 0) return 1;
  long unsigned int length = 0;
  while(number > 0) {
    number /= 10;
    length++;
  }
  return length;
}

/// Generate the name of a backup file by appending a number and a backup extension to the original file path.
/// @param filepath The original filepath.
/// @param number_of_backup The backup number to append to the file name.
/// @return A newly allocated string containing the backup file name.
char* getNameOfBackupFile(char* filepath, int number_of_backup) {
  removeExtension(filepath);
  size_t filepath_len = strlen(filepath);
  size_t backup_ext_len = strlen(BACKUP_FILE);
  size_t backup_filepath_size = filepath_len + 1 + getLengthOfNumber(number_of_backup) + backup_ext_len + 1;
  char* backup_filepath = (char*)malloc(backup_filepath_size);
  snprintf(backup_filepath, backup_filepath_size, "%s-%d%s", filepath, number_of_backup, BACKUP_FILE);
  return backup_filepath;
}

int kvs_backup(char* filepath, int backups_already_done) {
  char* backup_filepath = getNameOfBackupFile(filepath, backups_already_done);
  int fd_bk;
  fd_bk = open(backup_filepath, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
  free(backup_filepath);
  if (fd_bk >= 0) {
    kvs_show(fd_bk);
    close(fd_bk);
  } else {
    return 1;
  }
  return 0;
}

int compare(const struct dirent **a, const struct dirent **b) {
  return strcmp((*a)->d_name, (*b)->d_name);
}
