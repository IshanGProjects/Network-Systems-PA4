#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <netinet/tcp.h>  // Include this header for TCP_NODELAY
#include <openssl/md5.h>

#define BUFFER_SIZE 1024
#define MAX_FILENAME 255
#define COMMAND_SIZE 10
#define NUM_SERVERS 4

typedef struct {
    char ip[50];
    int port;
} ServerConfig;

typedef struct {
    char command[COMMAND_SIZE];
    char filename[MAX_FILENAME];
    char data[BUFFER_SIZE];
    int data_size; // To handle partial writes
    int chunk_indexF;
    int server_indexF;
} Packet;

ServerConfig serverConfigs[NUM_SERVERS];
int serverCount = 0;

int send_to_server_put(int server_index, Packet);
void read_config(const char *filename);
int connect_to_server(char *ip, int port);
int send_to_server_put(int server_index, Packet packet);
void execute_put_command(const char *filename);  

void read_config(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Unable to open the configuration file");
        exit(EXIT_FAILURE);
    }

    char line[128];
    int serverIndex;
    serverCount = 0;  // Make sure to reset server count each time you read the config.

    while (fgets(line, sizeof(line), file)) {
        int result = sscanf(line, "server dfs%d %[^:]:%d", &serverIndex, serverConfigs[serverCount].ip, &serverConfigs[serverCount].port);
        if (result == 3) {
            printf("Read configuration - Server %d: IP %s, Port %d\n", serverIndex, serverConfigs[serverCount].ip, serverConfigs[serverCount].port);
            serverCount++;
        } else {
            fprintf(stderr, "Failed to parse line: %s\n", line);
        }
    }
    fclose(file);
    printf("Configuration loaded successfully.\nServer Count: %d\n", serverCount);
    for (int i = 0; i < serverCount; i++) {
        printf("Server %d: %s:%d\n", i, serverConfigs[i].ip, serverConfigs[i].port);
    }
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

int send_to_server_put(int server_index, Packet packet){
    //connect to server
    int sock = connect_to_server(serverConfigs[server_index].ip, serverConfigs[server_index].port);    
    printf("Server Index: %d\n", server_index);
    // Send the packet
    if (send(sock, &packet, sizeof(Packet), 0) < 0) {
        perror("Send failed");
        return -1;
    }
}

// void execute_put_command(int sock, const char *filename) {
void execute_put_command(const char *filename) {
    FILE *file = fopen(filename, "rb");
    if (!file) {
        perror("Failed to open file");
        return;
    }

    //Calculate the file size
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);
    int chunk_size = file_size / NUM_SERVERS;
    int last_chunk_size = chunk_size + (file_size % NUM_SERVERS);

    //Calculate Hash To Figure out Distribution Pattern
    unsigned char hash[MD5_DIGEST_LENGTH];
    MD5((unsigned char*)filename, strlen(filename), hash);
    int x = hash[0] % NUM_SERVERS;
    printf("Hash Pattern(X Value): %d\n", x);

    // Define chunk distribution based on the hash index x
     int server_pairs[4][4][2] = {
        {{0, 1}, {1, 2}, {2, 3}, {3, 0}},  // x = 0
        {{3, 0}, {0, 1}, {1, 2}, {2, 3}},  // x = 1
        {{2, 3}, {3, 0}, {0, 1}, {1, 2}},  // x = 2
        {{1, 2}, {2, 3}, {3, 0}, {0, 1}}   // x = 3
    };


    //Read into Chunks
    for(int i = 0; i < NUM_SERVERS; i++){

       for(int j = 0; j < 2; j++){
            //Figure out Chuck index
            int chunk_index = server_pairs[x][i][j];
            printf("Chunk Index: %d\n", chunk_index);
            //Figure out if we need a regular chunk or the last chunk
            int current_chunk_size = (chunk_index == NUM_SERVERS - 1) ? last_chunk_size : chunk_size;
            printf("Current Chunk Size: %d\n", current_chunk_size);
            //Seek into the file for the correct chunk
            fseek(file, chunk_index * chunk_size, SEEK_SET);
            long tracker = ftell(file);
            printf("Tracker: %ld\n", tracker); 
            //Create Packet
            Packet packet;
            memset(&packet, 0, sizeof(Packet));
            sprintf(packet.command,"%s","PUT");
            sprintf(packet.filename, "%s", filename);
            packet.chunk_indexF = chunk_index + 1;
            packet.server_indexF = i + 1;

            //Start Reading into the Packet
            int current_bytes_read = 0;

            int bufferToUse = BUFFER_SIZE;
            if(current_chunk_size < BUFFER_SIZE){
                bufferToUse = current_chunk_size;
            }


            while(current_bytes_read < current_chunk_size){
                int bytes_read = 0;
                bytes_read = fread(packet.data, 1, bufferToUse, file);
                printf("Bytes Read: %d\n", bytes_read);
                if(bytes_read <= 0){
                    break;
                }
                current_bytes_read += bytes_read;
                packet.data_size = bytes_read;
                send_to_server_put(i, packet);
            }

            // Indicate the end of data transfer
            Packet eofc_packet;
            memset(&eofc_packet, 0, sizeof(Packet));
            memset(eofc_packet.data, 0, BUFFER_SIZE);  // Ensure buffer is clear before setting EO
            sprintf(eofc_packet.command, "%s", "PUT");
            sprintf(eofc_packet.filename, "%s", filename);
            sprintf(eofc_packet.data, "%s", "EOF");
            eofc_packet.chunk_indexF = chunk_index + 1;
            eofc_packet.server_indexF = i + 1;
            eofc_packet.data_size = strlen("EOF");
            send_to_server_put(i, eofc_packet);

            //Rewind the file pointer
            fseek(file, 0, SEEK_SET);
        }
    }
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
        if (strcmp(cmd, "PUT") == 0 && filename != NULL) {
            printf("Executing PUT command\n");
            execute_put_command(filename);
        }
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
