#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h> // Certifique-se de incluir a biblioteca pthread

#include "constants.h"
#include "io.h"
#include "kvs.h"
#include "operations.h"

static struct HashTable *kvs_table = NULL;

pthread_mutex_t subscriptions_mutex = PTHREAD_MUTEX_INITIALIZER;


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

  kvs_table = create_hash_table();
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

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_wrlock(&kvs_table->tablelock);

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write key pair (%s,%s)\n", keys[i], values[i]);
    }
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  
  pthread_rwlock_rdlock(&kvs_table->tablelock);

  write_str(fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    char *result = read_pair(kvs_table, keys[i]);
    char aux[MAX_STRING_SIZE];
    if (result == NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s,KVSERROR)", keys[i]);
    } else {
      snprintf(aux, MAX_STRING_SIZE, "(%s,%s)", keys[i], result);
    }
    write_str(fd, aux);
    free(result);
  }
  write_str(fd, "]\n");
  
  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  
  pthread_rwlock_wrlock(&kvs_table->tablelock);

  int aux = 0;
  for (size_t i = 0; i < num_pairs; i++) {
      if (!aux) {
        write_str(fd, "[");
        aux = 1;
      }
      char str[MAX_STRING_SIZE];
      snprintf(str, MAX_STRING_SIZE, "(%s,KVSMISSING)", keys[i]);
      write_str(fd, str);
  }
  if (aux) {
    write_str(fd, "]\n");
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

void kvs_show(int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return;
  }
  
  pthread_rwlock_rdlock(&kvs_table->tablelock);
  char aux[MAX_STRING_SIZE];
  
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
    while (keyNode != NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s, %s)\n", keyNode->key, keyNode->value);
      write_str(fd, aux);
      keyNode = keyNode->next; // Move to the next node of the list
    }
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
}

int kvs_backup(size_t num_backup,char* job_filename , char* directory) {
  pid_t pid;
  char bck_name[50];
  snprintf(bck_name, sizeof(bck_name), "%s/%s-%ld.bck", directory, strtok(job_filename, "."),
           num_backup);

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  pid = fork();
  pthread_rwlock_unlock(&kvs_table->tablelock);
  if (pid == 0) {
    // functions used here have to be async signal safe, since this
    // fork happens in a multi thread context (see man fork)
    int fd = open(bck_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    for (int i = 0; i < TABLE_SIZE; i++) {
      KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
      while (keyNode != NULL) {
        char aux[MAX_STRING_SIZE];
        aux[0] = '(';
        size_t num_bytes_copied = 1; // the "("
        // the - 1 are all to leave space for the '/0'
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        keyNode->key, MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        ", ", MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        keyNode->value, MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        ")\n", MAX_STRING_SIZE - num_bytes_copied - 1);
        aux[num_bytes_copied] = '\0';
        write_str(fd, aux);
        keyNode = keyNode->next; // Move to the next node of the list
      }
    }
    exit(1);
  } else if (pid < 0) {
    return -1;
  }
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}


#include "operations.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Definição das funções auxiliares (exemplo para subscribe_client, publish_message, etc.)
typedef struct Subscription {
    char key[MAX_KEY_LENGTH];
    int fd; // FIFO de respostas do cliente
    struct Subscription* next;
} Subscription;

static Subscription* subscriptions = NULL;

// Inscreve um cliente em uma chave
void subscribe_client(Session* s, const char* key) {
    pthread_mutex_lock(&subscriptions_mutex);

    // Verifica se o cliente já está inscrito na chave
    Subscription* current = subscriptions;
    while (current) {
        if (strcmp(current->key, key) == 0 && current->fd == s->fd_responses) {
            printf("Cliente já inscrito na chave %s\n", key);
            pthread_mutex_unlock(&subscriptions_mutex);
            return;
        }
        current = current->next;
    }

    // Adiciona nova inscrição
    Subscription* new_subscription = malloc(sizeof(Subscription));
    if (!new_subscription) {
        perror("Erro ao alocar nova inscrição");
        pthread_mutex_unlock(&subscriptions_mutex);
        return;
    }
    strncpy(new_subscription->key, key, MAX_KEY_LENGTH);
    new_subscription->fd = s->fd_responses;
    new_subscription->next = subscriptions;
    subscriptions = new_subscription;

    printf("Cliente inscrito na chave %s com fd=%d\n", key, s->fd_responses);
    pthread_mutex_unlock(&subscriptions_mutex);
}





// Cancela a inscrição de um cliente em uma chave
void unsubscribe_client(Session* s, const char* key) {
    pthread_mutex_lock(&subscriptions_mutex);

    Subscription** current = &subscriptions;
    while (*current) {
        if (strcmp((*current)->key, key) == 0 && (*current)->fd == s->fd_responses) {
            Subscription* to_remove = *current;
            *current = (*current)->next;
            free(to_remove);
            printf("Cliente removido da inscrição na chave %s\n", key);
            pthread_mutex_unlock(&subscriptions_mutex);
            return;
        }
        current = &((*current)->next);
    }

    printf("Cliente não encontrado na chave %s para cancelamento\n", key);
    pthread_mutex_unlock(&subscriptions_mutex);
}





// Cancela todas as inscrições de um cliente
void unsubscribe_all(Session* s) {
    pthread_mutex_lock(&subscriptions_mutex);
    Subscription** current = &subscriptions;
    while (*current) {
        if ((*current)->fd == s->fd_responses) {
            Subscription* to_remove = *current;
            *current = (*current)->next;
            free(to_remove);
        } else {
            current = &((*current)->next);
        }
    }
    pthread_mutex_unlock(&subscriptions_mutex);
}


// Publica uma mensagem para todos os inscritos em uma chave
void publish_message(const char* key, const char* message, int sender_fd) {
    pthread_mutex_lock(&subscriptions_mutex);

    printf("Publicando mensagem na chave %s: %s\n", key, message);
    Subscription* current = subscriptions;
    while (current) {
        if (strcmp(current->key, key) == 0 && current->fd != sender_fd) {
            dprintf(current->fd, "%s\n", message); // Envia a mensagem
            printf("Mensagem enviada para fd=%d\n", current->fd); // Log único
        }
        current = current->next;
    }

    pthread_mutex_unlock(&subscriptions_mutex);
}



