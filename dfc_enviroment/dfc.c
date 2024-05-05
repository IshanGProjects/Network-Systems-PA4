#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <netinet/tcp.h>  // Include this header for TCP_NODELAY

#define BUFFER_SIZE 1024
#define MAX_FILENAME 255
#define COMMAND_SIZE 10
#define MAX_SERVERS 10

typedef struct {
    char ip[50];
    int port;
} ServerConfig;

typedef struct {
    char command[COMMAND_SIZE];
    char filename[MAX_FILENAME];
    char data[BUFFER_SIZE];
    int data_size; // To handle partial writes
} Packet;

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

    // Disable Nagle's Algorithm
    int flag = 1;
    int result = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));
    if (result < 0) {
        perror("setsockopt(TCP_NODELAY) failed");
        close(sock);
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

// void execute_put_command(int sock, const char *filename) {
//     FILE *file = fopen(filename, "rb");
//     if (!file) {
//         perror("Failed to open file");
//         return;
//     }

//     fseek(file, 0, SEEK_END);
//     long fsize = ftell(file);
//     fseek(file, 0, SEEK_SET);

//     char *data = malloc(fsize + 1);
//     if (!data) {
//         perror("Memory allocation failed");
//         fclose(file);
//         return;
//     }
//     fread(data, 1, fsize, file);
//     fclose(file);
//     data[fsize] = '\0'; // Ensure string is null-terminated

//     // Construct the command with the data enclosed in quotes
//     char *command = malloc(strlen(filename) + strlen(data) + 50);
//     if (!command) {
//         perror("Memory allocation failed for command");
//         free(data);
//         return;
//     }
//     sprintf(command, "PUT %s \"%s\"", filename, data);
//     printf("Command: %s\n", command);
//     if (send(sock, command, strlen(command), 0) < 0) {
//         puts("Send failed");
//     }

//     free(command);
//     free(data);
// }

// void execute_put_command(int sock, const char *filename) {
//     FILE *file = fopen(filename, "rb");
//     if (!file) {
//         perror("Failed to open file");
//         return;
//     }

//     // Send initial command with filename
//     char init_command[1024];
//     sprintf(init_command, "PUT %s \"", filename);  // Start of data indicated by a quote
//     printf("Command: %s\n", init_command);
//     send(sock, init_command, strlen(init_command), 0);

//     char buffer[BUFFER_SIZE];
//     size_t bytes_read;
//     while ((bytes_read = fread(buffer, 1, BUFFER_SIZE, file)) > 0) {
//         send(sock, buffer, bytes_read, 0);
//     }

//     // Send ending quote to indicate the end of data
//     char end_quote[2] = "\"";
//     printf("End of data: %s\n", end_quote);
//     send(sock, end_quote, 1, 0);

//     fclose(file);
// }

void execute_put_command(int sock, const char *filename) {
    FILE *file = fopen(filename, "rb");
    if (!file) {
        perror("Failed to open file");
        return;
    }

    Packet packet;
    size_t bytes_read;

    memset(&packet, 0, sizeof(Packet));

    // Prepare the packet for the PUT command
    strcpy(packet.command, "PUT");
    strncpy(packet.filename, filename, MAX_FILENAME);

    while ((bytes_read = fread(packet.data, 1, BUFFER_SIZE, file)) > 0) {
        packet.data_size = bytes_read; // Set the data_size field
        if (send(sock, &packet, sizeof(Packet), 0) < 0) {
            perror("Send failed");
            break;
        }
        memset(packet.data, 0, BUFFER_SIZE);  // Ensure buffer is clear before setting EO
    }
    printf("End of data\n");
    memset(packet.data, 0, BUFFER_SIZE);  // Ensure buffer is clear before setting EO
    printf("Data at the Supposed end of data: %s\n", packet.data);
    // Indicate the end of data transfer
    strcpy(packet.data, "EOF"); // Use any unique sequence to indicate EOF
    packet.data_size = strlen(packet.data); // Set the data_size field
    printf("Data now sending EOF: %s\n", packet.data);
    send(sock, &packet, sizeof(Packet), 0); // Send the complete packet

    fclose(file);
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

void execute_command(const char *cmd, char *filename) {
    for (int i = 0; i < serverCount; i++) {
        int sock = connect_to_server(serverConfigs[i].ip, serverConfigs[i].port);
        if (sock < 0) {
            printf("Failed to connect to server %d\n", i + 1);
            continue;
        }

        if (strcmp(cmd, "PUT") == 0 && filename != NULL) {
            printf("Executing PUT command\n");
            execute_put_command(sock, filename);
        }else {
            printf("Executing other command\n");
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
