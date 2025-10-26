#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <dirent.h>
#include <errno.h>
#include <pthread.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "kvs.h"

/// Mutex to protect the number of active threads
pthread_mutex_t count_mutex;
pthread_cond_t count_cond;
int active_threads = 0;
int max_threads = 0;

/// Mutexes and conditions to protect command execution
pthread_mutex_t command_mutex;
pthread_cond_t command_cond;
int read_show_active = 0;
int write_delete_active = 0;

/// Limit the number of backups
int max_backups = 0;
int pids_count = 0;

/// Wait for a read/show/backup operation to finish
void read_show_backup_wait() {
  pthread_mutex_lock(&command_mutex);
  while (read_show_active > 0) {
      pthread_cond_wait(&command_cond, &command_mutex);
  }
  write_delete_active++;
  pthread_mutex_unlock(&command_mutex);
}

/// Handle finished write/delete operations
void write_delete_finished() {
  pthread_mutex_lock(&command_mutex);
  write_delete_active--;
  pthread_cond_broadcast(&command_cond);
  pthread_mutex_unlock(&command_mutex);
}

/// Wait for a write/delete operation to finish
void write_delete_wait() {
  pthread_mutex_lock(&command_mutex);
  while (write_delete_active > 0) {
    pthread_cond_wait(&command_cond, &command_mutex);
  }
  read_show_active++;
  pthread_mutex_unlock(&command_mutex);
}

/// Handle finished read/show operations
void read_show_finished() {
  pthread_mutex_lock(&command_mutex);
  read_show_active--;
  pthread_cond_broadcast(&command_cond);
  pthread_mutex_unlock(&command_mutex);
}

/// Process input from the user
/// @param fd File descriptor to read from
/// @param fd_out File descriptor to write to
/// @param filepath Filepath (Job File) for backups to process
void process_input(int fd, int fd_out, char* filepath) {
  int num_backups = 1;

  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;
    
    switch (get_next(fd)) {
      case CMD_WRITE:
        read_show_backup_wait();
        num_pairs = parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          pthread_mutex_lock(&command_mutex);
          write_delete_active--;
          pthread_cond_broadcast(&command_cond);
          pthread_mutex_unlock(&command_mutex);
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }
        write_delete_finished();
        break;

      case CMD_READ:
        write_delete_wait();
        num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          read_show_finished();
          continue;
        }

        if (kvs_read(num_pairs, keys, fd_out)) {
          fprintf(stderr, "Failed to read pair\n");
          read_show_finished();
        }

        read_show_finished();
        break;

      case CMD_DELETE:
        read_show_backup_wait();

        num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          write_delete_finished();
          continue;
        }

        if (kvs_delete(num_pairs, keys, fd_out)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        write_delete_finished();
        break;

      case CMD_SHOW:
        write_delete_wait();
        kvs_show(fd_out);
        read_show_finished();
        break;

      case CMD_WAIT:

        if (parse_wait(fd, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          if (write_all(fd_out, "Waiting...\n", 11) < 0) {
            fprintf(stderr, "Failed to write to output file\n");
            continue;
          }
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        write_delete_wait();
        pthread_mutex_lock(&command_mutex);
        while (pids_count >= max_backups) {
          int status;
          pid_t finished_pid = wait(&status);
          if (finished_pid > 0) {
            pids_count--;
          }
        }
        pids_count++;
        pthread_mutex_unlock(&command_mutex);
        
        pid_t pid = fork();
        if (pid == 0) { // Child Process
          if (kvs_backup(filepath, num_backups)) {
            fprintf(stderr, "Failed to perform backup.\n");
            _exit(1);
          }
          _exit(0);
          
        } else if (pid > 0) { // Main Process
          pthread_mutex_lock(&command_mutex);
          num_backups++;
          pthread_mutex_unlock(&command_mutex);

          // Check if child processes are finished
          while (waitpid(-1, NULL, WNOHANG) > 0) {
              pthread_mutex_lock(&command_mutex);
              pids_count--;
              pthread_mutex_unlock(&command_mutex);
          }
          read_show_finished();
        } else {
          fprintf(stderr, "Fork failed\n");
          read_show_finished();
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        // Not needed
        break;
        
      case CMD_EMPTY:
        break;

      case EOC:
        return;
    }
  }
}

/// Thread function to call process_input
/// @param path Filepath to process
void *process_input_thread_fn(void *path) {
  int fd, fd_out;
  char* filepath = (char*)path;
  fd = open(filepath, O_RDONLY);
  if (fd >= 0) {
    char* output_filepath = changeFileExtension(filepath, OUT_FILE);
    if (output_filepath == NULL) {
      close(fd);
    } else {
      fd_out = open(output_filepath, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
      free(output_filepath);
      if (fd_out >= 0) {
        process_input(fd, fd_out, filepath);
        close(fd_out);
      }
    }
    close(fd);
  }
  free(filepath);

  // Signal that the thread is done
  pthread_mutex_lock(&count_mutex);
  active_threads--;
  pthread_cond_signal(&count_cond);
  pthread_mutex_unlock(&count_mutex);

  return NULL;
}

/// Read all job files in the directory
/// @param job_dir Directory to read job files from
int readJobFiles(char* job_dir) {
  struct dirent *entry;
  DIR *dir;
  dir = opendir(job_dir);
  if (dir == NULL) return 1;
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0 && strlen(entry->d_name) <= MAX_JOB_FILE_NAME_SIZE && isExtensionFile(entry->d_name, JOB_FILE)) {
      size_t filepath_size = sizeof(char)*(strlen(job_dir) + 1 + MAX_JOB_FILE_NAME_SIZE + 1);
      char* filepath = (char*) malloc(filepath_size);
      snprintf(filepath, filepath_size, "%s/%s", job_dir, entry->d_name);
      pthread_mutex_lock(&count_mutex);
      while (active_threads >= max_threads) {
        pthread_cond_wait(&count_cond, &count_mutex);
      }
      active_threads++;
      pthread_mutex_unlock(&count_mutex);
      pthread_t thread;
      while (1) {
        int create_ret = pthread_create(&thread, NULL, process_input_thread_fn, (void *)filepath);
        if (create_ret == 0) {
            break;
        }
        fprintf(stderr, "Failed to create thread\n");
      }
      pthread_detach(thread);
    }
  }
  closedir(dir);
  pthread_mutex_lock(&count_mutex);
  while (active_threads > 0) {
    pthread_cond_wait(&count_cond, &count_mutex);
  }
  pthread_mutex_unlock(&count_mutex);
  return 0;
}

int main(int argc, char *argv[]) {
  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }
  
  if (argc != 4) {
    return 1;
  } else if (argc == 4) {
    // Maximum number of backups
    max_backups = atoi(argv[2]);
    if (max_backups <= 0) {
      return 1;
    }
    // Maximum number of threads
    max_threads = atoi(argv[3]);
    if (max_threads <= 0) {
      return 1;
    }
    // Initialize the threads
    pthread_mutex_init(&count_mutex, NULL);
    pthread_cond_init(&count_cond, NULL);
    pthread_mutex_init(&command_mutex, NULL);
    pthread_cond_init(&command_cond, NULL);
    
    int execution = readJobFiles(argv[1]);

    // Destroy kvs and threads
    pthread_mutex_destroy(&count_mutex);
    pthread_cond_destroy(&count_cond);
    pthread_mutex_destroy(&command_mutex);
    pthread_cond_destroy(&command_cond);
    kvs_terminate();

    return execution;
  }
}
