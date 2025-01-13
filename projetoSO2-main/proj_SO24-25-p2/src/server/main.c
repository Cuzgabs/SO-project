#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include <semaphore.h>  // Para usar semáforos
#include <sys/signal.h>
#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include <linux/limits.h>

// ---------------------------------------------------
// DEFINES e ESTRUTURAS GLOBAIS
// ---------------------------------------------------

// Número máximo de sessões simultâneas que o servidor suporta
#define MAX_SESSIONS 5


// Defina a struct ANTES de qualquer função que a use
struct SharedData {
    DIR* dir;
    char* dir_name;
    pthread_mutex_t directory_mutex;
};


// Exemplo de struct Subscription (simplificado)
typedef struct Subscription {
    char key[MAX_KEY_LENGTH];
    struct Subscription* next;
} Subscription;

// Declara a lista global de subscrições e seu mutex
static Subscription* subscriptions = NULL;
static pthread_mutex_t subscriptions_mutex = PTHREAD_MUTEX_INITIALIZER;


// Estrutura para rastrear uma sessão **ativa** globalmente (se você quiser)
typedef struct {
    int active;           // 1 se a sessão está ativa
    int fd_requests;      // descritor do FIFO de pedidos
    int fd_responses;     // descritor do FIFO de respostas
    char fifo_req[PATH_MAX];
    char fifo_res[PATH_MAX];
} ActiveSession;

// Array global de sessões + mutex (caso queira encerrar todas ao receber SIGUSR1)
static ActiveSession all_sessions[MAX_SESSIONS];
static pthread_mutex_t sessions_mutex = PTHREAD_MUTEX_INITIALIZER;

// Estrutura para guardar o pedido de sessão (nomes dos FIFOs, etc.)
// que iremos "produzir" na tarefa anfitriã e "consumir" nas threads gestoras
typedef struct {
    int fd_requests;      
    int fd_responses;
    char fifo_req[PATH_MAX];
    char fifo_res[PATH_MAX];
} SessionRequest;

// Buffer produtor-consumidor: um array de SessionRequest
static SessionRequest session_buffer[MAX_SESSIONS];
static int buf_in = 0;
static int buf_out = 0;
static int buf_count = 0;

// Mutex + semáforos para o buffer
static pthread_mutex_t buf_mutex = PTHREAD_MUTEX_INITIALIZER;
static sem_t sem_empty;  
static sem_t sem_full;   

// ---------------------------------------------------
// VARIÁVEIS GLOBAIS JÁ EXISTENTES
// ---------------------------------------------------
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;     
size_t max_backups;
size_t max_threads;
char* jobs_directory = NULL;
char* fifo_path = NULL;
int fifo_fd = -1;

// ---------------------------------------------------
// FUNÇÕES DO BUFFER
// ---------------------------------------------------
static void buffer_init() {
    sem_init(&sem_empty, 0, MAX_SESSIONS); // Começa com MAX_SESSIONS posições vazias
    sem_init(&sem_full, 0, 0);            // Começa sem itens
}

static void buffer_put(const SessionRequest* req) {
    sem_wait(&sem_empty);                // Espera ter espaço
    pthread_mutex_lock(&buf_mutex);

    session_buffer[buf_in] = *req;
    buf_in = (buf_in + 1) % MAX_SESSIONS;
    buf_count++;

    pthread_mutex_unlock(&buf_mutex);
    sem_post(&sem_full);                 // Sinaliza que há um item
}

static SessionRequest buffer_get() {
    SessionRequest req;

    sem_wait(&sem_full);                 // Espera ter algo no buffer
    pthread_mutex_lock(&buf_mutex);

    req = session_buffer[buf_out];
    buf_out = (buf_out + 1) % MAX_SESSIONS;
    buf_count--;

    pthread_mutex_unlock(&buf_mutex);
    sem_post(&sem_empty);                // Sinaliza que uma posição foi liberada

    return req;
}

// ---------------------------------------------------
// LIMPEZA DO FIFO AO ENCERRAR
// ---------------------------------------------------
void cleanup_fifo() {
    if (fifo_fd != -1) {
        close(fifo_fd);
    }
    if (fifo_path != NULL) {
        printf("Removendo FIFO no encerramento: %s\n", fifo_path);
        unlink(fifo_path);
    }
}

// ---------------------------------------------------
// FUNÇÕES AUXILIARES EXISTENTES
// ---------------------------------------------------

// Handler de SIGINT
void my_sigint_handler(int signum) {
    (void) signum;  // evita warning “unused parameter”
    printf("Recebi SIGINT, encerrando servidor...\n");

    // Se quiser, chame suas rotinas de limpeza:
    cleanup_fifo();    // por exemplo
    kvs_terminate();   // se quiser encerrar o KVS

    exit(0);           // finaliza o processo imediatamente
}




int filter_job_files(const struct dirent* entry) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot != NULL && strcmp(dot, ".job") == 0) {
        return 1;  // Keep this file (it has the .job extension)
    }
    return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
        return 1;
    }

    if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
        fprintf(stderr, "%s/%s\n", dir, entry->d_name);
        return 1;
    }

    strcpy(in_path, dir);
    strcat(in_path, "/");
    strcat(in_path, entry->d_name);

    strcpy(out_path, in_path);
    strcpy(strrchr(out_path, '.'), ".out");

    return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
    size_t file_backups = 0;
    while (1) {
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;

        switch (get_next(in_fd)) {
            case CMD_WRITE: {
                num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_write(num_pairs, keys, values)) {
                    write_str(STDERR_FILENO, "Failed to write pair\n");
                }
                break;
            }
            case CMD_READ: {
                num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_read(num_pairs, keys, out_fd)) {
                    write_str(STDERR_FILENO, "Failed to read pair\n");
                }
                break;
            }
            case CMD_DELETE: {
                num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_delete(num_pairs, keys, out_fd)) {
                    write_str(STDERR_FILENO, "Failed to delete pair\n");
                }
                break;
            }
            case CMD_SHOW: {
                kvs_show(out_fd);
                break;
            }
            case CMD_WAIT: {
                if (parse_wait(in_fd, &delay, NULL) == -1) {
                    write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (delay > 0) {
                    printf("Waiting %d seconds\n", delay / 1000);
                    kvs_wait(delay);
                }
                break;
            }
            case CMD_BACKUP: {
                pthread_mutex_lock(&n_current_backups_lock);
                if (active_backups >= max_backups) {
                    wait(NULL);
                } else {
                    active_backups++;
                }
                pthread_mutex_unlock(&n_current_backups_lock);
                int aux = kvs_backup(++file_backups, filename, jobs_directory);
                if (aux < 0) {
                    write_str(STDERR_FILENO, "Failed to do backup\n");
                } else if (aux == 1) {
                    return 1;
                }
                break;
            }
            case CMD_INVALID: {
                write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
                break;
            }
            case CMD_HELP: {
                write_str(STDOUT_FILENO,
                    "Available commands:\n"
                    "  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n" // Not implemented
                    "  HELP\n");
                break;
            }
            case CMD_EMPTY: {
                break;
            }
            case EOC: {
                printf("EOF\n");
                return 0;
            }
        }
    }
}

// Thread que processa os .job files
static void* get_file(void* arguments) {
    struct SharedData* thread_data = (struct SharedData*) arguments;
    DIR* dir = thread_data->dir;
    char* dir_name = thread_data->dir_name;

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
        fprintf(stderr, "Thread failed to lock directory_mutex\n");
        return NULL;
    }

    struct dirent* entry;
    char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
    while ((entry = readdir(dir)) != NULL) {
        if (entry_files(dir_name, entry, in_path, out_path)) {
            continue;
        }

        if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
            fprintf(stderr, "Thread failed to unlock directory_mutex\n");
            return NULL;
        }

        int in_fd = open(in_path, O_RDONLY);
        if (in_fd == -1) {
            write_str(STDERR_FILENO, "Failed to open input file: ");
            write_str(STDERR_FILENO, in_path);
            write_str(STDERR_FILENO, "\n");
            pthread_exit(NULL);
        }

        int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (out_fd == -1) {
            write_str(STDERR_FILENO, "Failed to open output file: ");
            write_str(STDERR_FILENO, out_path);
            write_str(STDERR_FILENO, "\n");
            pthread_exit(NULL);
        }

        int out = run_job(in_fd, out_fd, entry->d_name);

        close(in_fd);
        close(out_fd);

        if (out) {
            if (closedir(dir) == -1) {
                fprintf(stderr, "Failed to close directory\n");
                return 0;
            }
            exit(0);
        }

        if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
            fprintf(stderr, "Thread failed to lock directory_mutex\n");
            return NULL;
        }
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
        fprintf(stderr, "Thread failed to unlock directory_mutex\n");
        return NULL;
    }

    pthread_exit(NULL);
}

// Cria várias threads para processar os .job files
static void dispatch_threads(DIR* dir) {
    pthread_t* threads = malloc(max_threads * sizeof(pthread_t));
    if (threads == NULL) {
        fprintf(stderr, "Failed to allocate memory for threads\n");
        return;
    }

    struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};

    for (size_t i = 0; i < max_threads; i++) {
        if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
            fprintf(stderr, "Failed to create thread %zu\n", i);
            pthread_mutex_destroy(&thread_data.directory_mutex);
            free(threads);
            return;
        }
    }

    for (unsigned int i = 0; i < max_threads; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            fprintf(stderr, "Failed to join thread %u\n", i);
            pthread_mutex_destroy(&thread_data.directory_mutex);
            free(threads);
            return;
        }
    }

    if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
        fprintf(stderr, "Failed to destroy directory_mutex\n");
    }

    free(threads);
}

// ---------------------------------------------------
// FUNÇÕES NOVAS PARA CONEXÃO COM O CLIENTE
// ---------------------------------------------------

static void process_session(Session* s) {
    char buffer[128]; // Buffer para pedidos do cliente
    while (1) {
        ssize_t bytes_read = read(s->fd_requests, buffer, sizeof(buffer) - 1);
        if (bytes_read > 0) {
            buffer[bytes_read] = '\0'; // Garante que a string esteja terminada
            printf("Received request: %s\n", buffer);

            // Processa o comando DISCONNECT
            if (strcmp(buffer, "DISCONNECT\n") == 0) {
                printf("Client requested disconnect\n");
                break;  // Sai do loop ao receber DISCONNECT
            } 
            // Processa o comando SUBSCRIBE
            else if (strncmp(buffer, "SUBSCRIBE", 9) == 0) {
                char key[MAX_KEY_LENGTH];
                if (sscanf(buffer + 10, "%s", key) == 1) {
                    printf("Inscrevendo cliente na chave %s\n", key);
                    subscribe_client(s, key); // Função para inscrever o cliente
                    const char* response = "SUBSCRIBED\n";
                    if (write(s->fd_responses, response, strlen(response)) < 0) {
                        perror("Erro ao enviar resposta SUBSCRIBED");
                    }
                } else {
                    const char* error = "ERRO: Comando SUBSCRIBE malformado\n";
                    if (write(s->fd_responses, error, strlen(error)) < 0) {
                        perror("Erro ao enviar erro do comando SUBSCRIBE");
                    }
                }
            } 
            // Processa o comando PUBLISH
            else if (strncmp(buffer, "PUBLISH", 7) == 0) {
                char key[MAX_KEY_LENGTH], message[MAX_MESSAGE_LENGTH];
                if (sscanf(buffer + 8, "%s %[^\n]", key, message) == 2) {
                    printf("Publicando mensagem na chave %s: %s\n", key, message);
                    publish_message(key, message, s->fd_responses); // Passa o descritor do cliente que publicou
                    const char* response = "MESSAGE PUBLISHED\n";
                    if (write(s->fd_responses, response, strlen(response)) < 0) {
                        perror("Erro ao enviar resposta MESSAGE PUBLISHED");
                    }
                } else {
                    const char* error = "ERRO: Comando PUBLISH malformado\n";
                    if (write(s->fd_responses, error, strlen(error)) < 0) {
                        perror("Erro ao enviar erro do comando PUBLISH");
                    }
                }
            }

            // Processa o comando UNSUBSCRIBE
            else if (strncmp(buffer, "UNSUBSCRIBE", 11) == 0) {
                char key[MAX_KEY_LENGTH];
                if (sscanf(buffer + 12, "%s", key) == 1) {
                    printf("Cancelando inscrição do cliente na chave %s\n", key);
                    unsubscribe_client(s, key); // Função para remover inscrição
                    const char* response = "UNSUBSCRIBED\n";
                    if (write(s->fd_responses, response, strlen(response)) < 0) {
                        perror("Erro ao enviar resposta UNSUBSCRIBED");
                    }
                } else {
                    const char* error = "ERRO: Comando UNSUBSCRIBE malformado\n";
                    if (write(s->fd_responses, error, strlen(error)) < 0) {
                        perror("Erro ao enviar erro do comando UNSUBSCRIBE");
                    }
                }
            } 
            // Comando desconhecido
            else {
                const char* unknown = "UNKNOWN COMMAND\n";
                if (write(s->fd_responses, unknown, strlen(unknown)) < 0) {
                    perror("Erro ao enviar comando desconhecido");
                }
            }
        } else if (bytes_read == 0) {
            // FIFO fechado pelo cliente (EOF)
            printf("FIFO fechado pelo cliente. Finalizando sessão.\n");
            break;  // Sai do loop ao detectar EOF
        } else {
            perror("Erro ao ler pedido do cliente");
            break;  // Sai do loop em caso de erro
        }
    }

    // Limpeza da sessão
    printf("Limpando sessão do cliente...\n");
    close(s->fd_requests);
    close(s->fd_responses);

    // Remove os FIFOs do cliente, verificando antes se eles ainda existem
    if (access(s->fifo_requests, F_OK) == 0) {
        if (unlink(s->fifo_requests) == 0) {
            printf("FIFO de pedidos removido: %s\n", s->fifo_requests);
        } else {
            perror("Erro ao remover FIFO de pedidos");
        }
    } else {
        printf("FIFO de pedidos já removido: %s\n", s->fifo_requests);
    }

    if (access(s->fifo_responses, F_OK) == 0) {
        if (unlink(s->fifo_responses) == 0) {
            printf("FIFO de respostas removido: %s\n", s->fifo_responses);
        } else {
            perror("Erro ao remover FIFO de respostas");
        }
    } else {
        printf("FIFO de respostas já removido: %s\n", s->fifo_responses);
    }

    // Remove inscrições ativas do cliente
    unsubscribe_all(s);

    free(s);
}



void cleanup_all_sessions() {
    // Para cada Session ativa, feche requests, responses, notifications...
    // Por ex, se você guarda um array global de Sessions ou uma linked list
    pthread_mutex_lock(&sessions_mutex);
    for (int i = 0; i < MAX_SESSIONS; i++) {
        if (all_sessions[i].active) {
            close(all_sessions[i].fd_requests);
            close(all_sessions[i].fd_responses);
            // se tiver notifications: close(all_sessions[i].fd_notif);

            // unlink os FIFOs do cliente se for da responsabilidade do servidor
            // ...
            all_sessions[i].active = 0;
        }
    }
    pthread_mutex_unlock(&sessions_mutex);
}


void cleanup_all_subscriptions() {
    pthread_mutex_lock(&subscriptions_mutex);
    while (subscriptions) {
        Subscription* tmp = subscriptions;
        subscriptions = subscriptions->next;
        free(tmp);
    }
    pthread_mutex_unlock(&subscriptions_mutex);
}



void add_subscription(const char* key) {
    pthread_mutex_lock(&subscriptions_mutex);

    Subscription* new_sub = malloc(sizeof(Subscription));
    strncpy(new_sub->key, key, MAX_KEY_LENGTH);
    new_sub->next = subscriptions;
    subscriptions = new_sub;

    pthread_mutex_unlock(&subscriptions_mutex);
}



void* signal_waiter_func(void* arg) {
    (void)arg; // elimina warning "unused parameter"

    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);

    while (1) {
        int sig;
        if (sigwait(&set, &sig) == 0) {
            if (sig == SIGUSR1) {
                printf("[Tarefa Anfitriã] Recebi SIGUSR1, limpando tudo!\n");
                cleanup_all_sessions();        // Se quiser fechar as sessões
                cleanup_all_subscriptions();   // Se quiser limpar subscrições
            }
        }
    }
    return NULL;
}






static void* accept_connections(void* arg) {
    (void)arg;  // Evita warning "unused parameter"

    // Desbloqueia SIGUSR1 só nesta thread (tarefa anfitriã),
    // pois no main() bloqueamos SIGUSR1 para todo o processo.
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    pthread_sigmask(SIG_UNBLOCK, &set, NULL);

    // Cria a thread que fará sigwait em SIGUSR1 (e.g. signal_waiter_func)
    pthread_t sig_thr;
    if (pthread_create(&sig_thr, NULL, signal_waiter_func, NULL) != 0) {
        perror("Falha ao criar thread de sigwait (SIGUSR1)");
        return NULL;
    }

    char buffer_line[512];

    // Loop principal de leitura do FIFO de registro
    while (1) {
        ssize_t count = read(fifo_fd, buffer_line, sizeof(buffer_line) - 1);
        if (count > 0) {
            buffer_line[count] = '\0';
            char* line = strtok(buffer_line, "\n");
            while (line) {
                printf("Mensagem recebida: %s\n", line);

                SessionRequest req;
                memset(&req, 0, sizeof(req));

                // Supõe que a linha seja "req_fifo;resp_fifo"
                if (sscanf(line, "%[^;];%s", req.fifo_req, req.fifo_res) == 2) {
                    // Verifica se os FIFOs existem
                    if (access(req.fifo_req, F_OK) == -1 || access(req.fifo_res, F_OK) == -1) {
                        fprintf(stderr, "FIFO de cliente não encontrado.\n");
                    } else {
                        // INSERE no buffer (produtor)
                        buffer_put(&req);
                        printf("Pedido de sessão inserido no buffer.\n");
                    }
                } else {
                    fprintf(stderr, "Mensagem malformada: %s\n", line);
                }
                line = strtok(NULL, "\n");
            }
        }
        else if (count == 0) {
            // FIFO vazio => dorme um pouco e continua
            struct timespec ts = {0, 500000000}; // 500ms
            nanosleep(&ts, NULL);
        }
        else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // Nenhuma mensagem no momento => idem
            struct timespec ts = {0, 500000000};
            nanosleep(&ts, NULL);
        }
        else {
            perror("Erro ao ler FIFO de registo");
            break;
        }
    }

    // Quando sair do while(1), por algum motivo, aguardamos a thread de sigwait
    pthread_join(sig_thr, NULL);
    return NULL;
}







static void* session_manager_func(void* arg) {
    (void)arg;  // evita warning "unused parameter"
    while (1) {
        // 1) Espera chegar um "pedido" do buffer
        SessionRequest req = buffer_get();
        printf("[Thread gestora] Peguei um pedido do buffer!\n");

        // 2) Cria a struct Session e abre os FIFOs
        Session* s = calloc(1, sizeof(Session));
        if (!s) {
            perror("Erro ao alocar Session");
            continue; // Tenta outro
        }
        s->client_pid = getpid();
        strncpy(s->fifo_requests, req.fifo_req, PATH_MAX);
        strncpy(s->fifo_responses, req.fifo_res, PATH_MAX);

        s->fd_requests = open(s->fifo_requests, O_RDWR); 
        s->fd_responses = open(s->fifo_responses, O_RDWR);

        if (s->fd_requests == -1 || s->fd_responses == -1) {
            perror("Erro ao abrir FIFOs do cliente");
            if (s->fd_requests != -1) close(s->fd_requests);
            if (s->fd_responses != -1) close(s->fd_responses);
            free(s);
            continue;
        }

        // 3) Processa a sessão (era session_thread_func)
        process_session(s);
        // Quando sair de process_session, a session já foi free() e FIFO fechado
        // Volta ao while(1) esperando o próximo pedido.
    }
    return NULL;
}




int main(int argc, char** argv) {
    // Bloqueia SIGUSR1 em TODO o processo
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    if (pthread_sigmask(SIG_BLOCK, &set, NULL) != 0) {
        perror("pthread_sigmask SIG_BLOCK");
        exit(EXIT_FAILURE);
    }

    // Parse de argumentos
    if (argc < 5) {
        write_str(STDERR_FILENO, "Usage: ");
        write_str(STDERR_FILENO, argv[0]);
        write_str(STDERR_FILENO, " <jobs_dir>");
        write_str(STDERR_FILENO, " <max_threads>");
        write_str(STDERR_FILENO, " <max_backups>");
        write_str(STDERR_FILENO, " <fifo_path>\n");
        return 1;
    }

    jobs_directory = argv[1];
    fifo_path      = argv[4];

    // Converte max_threads e max_backups
    char* endptr;
    max_threads  = strtoul(argv[2], &endptr, 10);
    if (*endptr != '\0') {
        fprintf(stderr, "Invalid max_threads value\n");
        return 1;
    }

    max_backups  = strtoul(argv[3], &endptr, 10);
    if (*endptr != '\0') {
        fprintf(stderr, "Invalid max_backups value\n");
        return 1;
    }

    if (max_backups <= 0) {
        write_str(STDERR_FILENO, "Invalid number of backups\n");
        return 0;
    }

    if (max_threads <= 0) {
        write_str(STDERR_FILENO, "Invalid number of threads\n");
        return 0;
    }

    // Inicia a KVS
    if (kvs_init()) {
        write_str(STDERR_FILENO, "Failed to initialize KVS\n");
        return 1;
    }

    // Instala um handler para SIGINT (por ex., para terminar se quiser)
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = my_sigint_handler;  // sua função
    sigaction(SIGINT, &sa, NULL);

    // Cria FIFO de registo
    printf("Tentando criar FIFO em: %s\n", fifo_path);
    if (mkfifo(fifo_path, 0666) == -1) {
        if (errno != EEXIST) {
            perror("Failed to create FIFO");
            exit(EXIT_FAILURE);
        } else {
            printf("FIFO já existia: %s\n", fifo_path);
        }
    } else {
        printf("FIFO criado com sucesso: %s\n", fifo_path);
    }

    printf("FIFO criado, verificando acesso: %s\n", fifo_path);
    if (access(fifo_path, F_OK) == -1) {
        perror("FIFO desapareceu antes de abrir");
        exit(EXIT_FAILURE);
    }

    // Verifica se o FIFO existe após criação
    if (access(fifo_path, F_OK) == -1) {
        perror("FIFO não encontrado após criação");
        exit(EXIT_FAILURE);
    }

    // Abre o FIFO de registro em modo não bloqueante
    fifo_fd = open(fifo_path, O_RDONLY | O_NONBLOCK);
    if (fifo_fd == -1) {
        perror("Failed to open FIFO");
        exit(EXIT_FAILURE);
    }
    printf("FIFO aberto com sucesso: %s\n", fifo_path);

    // Limpeza do FIFO quando sair
    atexit(cleanup_fifo);

    // Abre o diretório de .job
    DIR* dir = opendir(jobs_directory);
    if (!dir) {
        perror("Failed to open jobs directory");
        return 1;
    }

    // Processa .job (1ª parte)
    dispatch_threads(dir);

    if (closedir(dir) == -1) {
        perror("Failed to close jobs directory");
        return 0;
    }

    // Inicializa buffer produtor-consumidor
    buffer_init();

    // Cria as threads gestoras
    pthread_t managers[MAX_SESSIONS];
    for (int i = 0; i < MAX_SESSIONS; i++) {
        if (pthread_create(&managers[i], NULL, session_manager_func, NULL) != 0) {
            perror("Erro ao criar thread gestora");
            exit(EXIT_FAILURE);
        }
    }

    // Cria a thread anfitriã (aceita conexões)
    pthread_t host_thread;
    if (pthread_create(&host_thread, NULL, accept_connections, NULL) != 0) {
        perror("Erro ao criar thread anfitriã");
        exit(EXIT_FAILURE);
    }

    // Aguarda a thread anfitriã terminar (opcional)
    pthread_join(host_thread, NULL);

    // Se quiser, aguarda as threads gestoras
    for (int i = 0; i < MAX_SESSIONS; i++) {
        pthread_join(managers[i], NULL);
    }

    // Se houver backups ativos, espere
    while (active_backups > 0) {
        wait(NULL);
        active_backups--;
    }

    // Encerra KVS
    kvs_terminate();
    return 0;
}
