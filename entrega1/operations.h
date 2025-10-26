#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>
#include "kvs.h"

/// Writes all the bytes from a buffer to a file descriptor.
/// @param fd File descriptor to write to.
/// @param buf Buffer to write from.
/// @param len Number of bytes to write.
/// @return 0 if all bytes were written, -1 otherwise.
int write_all(int fd, const char *buf, size_t len);

/// Initializes the KVS state.
/// @return 0 if the KVS state was initialized successfully, 1 otherwise.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 if the KVS state was terminated successfully, 1 otherwise.
int kvs_terminate();

/// Writes a key value pair to the KVS. If key already exists it is updated.
/// @param num_pairs Number of pairs being written.
/// @param keys Array of keys' strings.
/// @param values Array of values' strings.
/// @return 0 if the pairs were written successfully, 1 otherwise.
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]);

/// Reads values from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write the (successful) output.
/// @return 0 if the key reading, 1 otherwise.
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd_out);

/// Deletes key value pairs from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd_out);

/// Writes the state of the KVS.
/// @param fd File descriptor to write the output.
void kvs_show(int fd_out);

/// Creates a backup of the KVS state and stores it in the correspondent
/// backup file
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_backup(char* filepath, int backups_already_done);

/// Waits for the last backup to be called.
void kvs_wait_backup();

/// Waits for a given amount of time.
/// @param delay_us Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

/// Check if a file has a specific extension.
/// @param filename Name of the file to check.
/// @param extension File extension (".job", ".out", ".bck").
/// @return 0 if the file doesn't have this extension, 1 otherwise.
int isExtensionFile(const char *filename, const char* extension);

/// Change the extension of a given filepath to the specified extension.
/// @param filepath The original file path.
/// @param extension The new extension to apply (".out", ".bck").
/// @return A newly allocated string containing the modified filepath with the new extension.
char* changeFileExtension(char* filepath, const char* extension);

/// Compare function for scandir.
/// @param a First dirent.
/// @param b Second dirent.
/// @return 0 if the first dirent is greater than the second, 1 otherwise.
int compare(const struct dirent **a, const struct dirent **b);

#endif  // KVS_OPERATIONS_H
