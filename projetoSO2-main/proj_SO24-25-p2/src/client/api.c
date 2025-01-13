#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <fcntl.h>       // Para open
#include <unistd.h>      // Para close, unlink
#include <sys/stat.h>    // Para mkfifo
#include <stdio.h>       // Para perror, fprintf, snprintf
#include <stdlib.h>      // Para exit, malloc
#include <string.h>      // Para strlen, strncpy

static int request_fd = -1;
static int response_fd = -1;
static int notification_fd = -1;

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path, int* notif_pipe) {
    // Criar FIFOs locais para pedidos, respostas e notificações
    if (mkfifo(req_pipe_path, 0666) == -1 || mkfifo(resp_pipe_path, 0666) == -1 || mkfifo(notif_pipe_path, 0666) == -1) {
        perror("Failed to create FIFOs");
        return 1;
    }

    // Abrir FIFOs
    request_fd = open(req_pipe_path, O_WRONLY);
    response_fd = open(resp_pipe_path, O_RDONLY | O_NONBLOCK);
    notification_fd = open(notif_pipe_path, O_RDONLY | O_NONBLOCK);

    if (request_fd == -1 || response_fd == -1 || notification_fd == -1) {
        perror("Failed to open FIFOs");
        return 1;
    }

    // Enviar mensagem de conexão para o servidor
    int server_fd = open(server_pipe_path, O_WRONLY);
    if (server_fd == -1) {
        perror("Failed to open server FIFO");
        return 1;
    }

    char message[128];
    snprintf(message, sizeof(message), "CONNECT|%s|%s|%s", req_pipe_path, resp_pipe_path, notif_pipe_path);

    if (write(server_fd, message, strlen(message)) == -1) {
        perror("Failed to send connect message");
        close(server_fd);
        return 1;
    }

    close(server_fd);
    *notif_pipe = notification_fd;
    return 0;
}

int kvs_disconnect(void) {
    if (request_fd == -1 || response_fd == -1) {
        fprintf(stderr, "No active connection\n");
        return 1;
    }

    // Enviar mensagem de desconexão
    char message[128] = "DISCONNECT";

    if (write(request_fd, message, strlen(message)) == -1) {
        perror("Failed to send disconnect message");
        return 1;
    }

    // Fechar e remover FIFOs
    close(request_fd);
    close(response_fd);
    close(notification_fd);

    request_fd = response_fd = notification_fd = -1;

    unlink("/tmp/req");
    unlink("/tmp/resp");
    unlink("/tmp/notif");

    return 0;
}

int kvs_subscribe(const char* key) {
    if (request_fd == -1 || response_fd == -1) {
        fprintf(stderr, "No active connection\n");
        return 1;
    }

    char message[128];
    snprintf(message, sizeof(message), "SUBSCRIBE|%s", key);

    if (write(request_fd, message, strlen(message)) == -1) {
        perror("Failed to send subscribe message");
        return 1;
    }

    return 0;
}

int kvs_unsubscribe(const char* key) {
    if (request_fd == -1 || response_fd == -1) {
        fprintf(stderr, "No active connection\n");
        return 1;
    }

    char message[128];
    snprintf(message, sizeof(message), "UNSUBSCRIBE|%s", key);

    if (write(request_fd, message, strlen(message)) == -1) {
        perror("Failed to send unsubscribe message");
        return 1;
    }

    return 0;
}
