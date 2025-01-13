#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>
#include <pthread.h>
#include <limits.h>
#include "constants.h"


// Definição da estrutura Session
typedef struct Session {
    int fd_requests;            // FIFO de pedidos
    int fd_responses;           // FIFO de respostas
    pthread_t tid;              // Thread associada ao cliente
    pid_t client_pid;           // PID do cliente
    char fifo_requests[PATH_MAX];   // Caminho do FIFO de pedidos
    char fifo_responses[PATH_MAX];  // Caminho do FIFO de respostas
} Session;

/**
 * Funções auxiliares para inscrição e publicação de mensagens:
 */
void subscribe_client(Session* s, const char* key);
void unsubscribe_client(Session* s, const char* key);
void publish_message(const char* key, const char* message, int sender_fd);
void unsubscribe_all(Session* s);

/**
 * Funções da KVS (Key-Value Store):
 */
int kvs_init();
int kvs_terminate();
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]);
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);
void kvs_show(int fd);
int kvs_backup(size_t num_backup, char* job_filename, char* directory);
void kvs_wait(unsigned int delay_ms);

// Você pode implementar getters/setters se desejar, como declarado inicialmente:
// void set_max_backups(int _max_backups);
// void set_n_current_backups(int _n_current_backups);
// int get_n_current_backups();

#endif // KVS_OPERATIONS_H
