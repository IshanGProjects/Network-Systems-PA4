#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>

#define BUFFER_SIZE 1024
#define MAX_SERVERS 10

typedef struct {
    char ip[50];
    int port;
} ServerConfig;

ServerConfig serverConfigs[MAX_SERVERS];
int serverCount = 0;

void read_config(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Unable to open the configuration file");
        exit(EXIT_FAILURE);
    }

    char line[128];
    while (fgets(line, sizeof(line), file)) {
        if (strncmp(line, "server", 6) == 0) {
            sscanf(line, "server dfs%d %[^:]:%d", &serverCount, serverConfigs[serverCount].ip, &serverConfigs[serverCount].port);
            serverCount++;
        }
    }
    fclose(file);
    printf("Configuration loaded successfully.\n");
}

int connect_to_server(char *ip, int port) {
    int sock;
    struct sockaddr_in server;

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        printf("Could not create socket");
        return -1;
    }

    server.sin_addr.s_addr = inet_addr(ip);
    server.sin_family = AF_INET;
    server.sin_port = htons(port);

    // Connect to remote server
    if (connect(sock, (struct sockaddr *)&server, sizeof(server)) < 0) {
        perror("connect failed. Error");
        return -1;
    }

    printf("Connected to server at %s:%d\n", ip, port);
    return sock;
}

// void send_command(int sock, const char *cmd) {
//     if (send(sock, cmd, strlen(cmd), 0) < 0) {
//         puts("Send failed");
//         return;
//     }
// }

void send_command(int sock, const char *cmd, const char *data) {
    if (data != NULL) {
        char *full_command = malloc(strlen(cmd) + strlen(data) + 2); // Extra space for space and null-terminator
        if (full_command == NULL) {
            perror("Memory allocation failed");
            return;
        }
        sprintf(full_command, "%s %s", cmd, data);
        if (send(sock, full_command, strlen(full_command), 0) < 0) {
            puts("Send failed");
        }
        free(full_command);
    } else {
        if (send(sock, cmd, strlen(cmd), 0) < 0) {
            puts("Send failed");
        }
    }
}

void execute_put_command(int sock, const char *filename) {
    FILE *file = fopen(filename, "rb");
    if (!file) {
        perror("Failed to open file");
        return;
    }

    fseek(file, 0, SEEK_END);
    long fsize = ftell(file);
    fseek(file, 0, SEEK_SET);  //same as rewind(file);

    char *string = malloc(fsize + 1);
    if (!string) {
        perror("Memory allocation failed");
        fclose(file);
        return;
    }
    fread(string, 1, fsize, file);
    fclose(file);

    string[fsize] = 0;

    send_command(sock, "PUT", string);
    free(string);
}

void receive_data(int sock) {
    char server_reply[BUFFER_SIZE];
    // Receive a reply from the server
    while(1) {
        int len = recv(sock, server_reply , BUFFER_SIZE , 0);
        if(len <= 0) break;
        fwrite(server_reply, len, 1, stdout);
    }
    puts("");
}

// void execute_command(const char *cmd, char *filename) {
//     for (int i = 0; i < serverCount; i++) {
//         int sock = connect_to_server(serverConfigs[i].ip, serverConfigs[i].port);
//         if (sock < 0) {
//             printf("Failed to connect to server %d\n", i + 1);
//             continue;
//         }

//         char full_command[BUFFER_SIZE];
//         sprintf(full_command, "%s %s", cmd, filename ? filename : "");
//         sleep(1); // Sleep for a second
//         send_command(sock, full_command);
//         printf("Command '%s' sent to server %d\n", cmd, i + 1);
//         receive_data(sock);
//         close(sock);
//         printf("Connection to server %d closed\n", i + 1);
//     }
// }

void execute_command(const char *cmd, char *filename) {
    for (int i = 0; i < serverCount; i++) {
        int sock = connect_to_server(serverConfigs[i].ip, serverConfigs[i].port);
        if (sock < 0) {
            printf("Failed to connect to server %d\n", i + 1);
            continue;
        }

        if (strcmp(cmd, "PUT") == 0 && filename != NULL) {
            execute_put_command(sock, filename);
        } else {
            send_command(sock, cmd, NULL);
        }

        receive_data(sock);
        close(sock);
        printf("Connection to server %d closed\n", i + 1);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: ./dfc <command> [filename]\n");
        return 1;
    }

    read_config("dfc.conf");

    if (strcmp(argv[1], "LIST") == 0 || strcmp(argv[1], "GET") == 0 || strcmp(argv[1], "PUT") == 0) {
        execute_command(argv[1], argc > 2 ? argv[2] : NULL);
    } else {
        printf("Invalid command\n");
    }

    return 0;
}
