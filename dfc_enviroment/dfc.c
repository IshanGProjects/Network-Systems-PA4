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
#define NUM_PARTS 4

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

#define MAX_FILES 100

typedef struct {
    char filename[MAX_FILENAME];
    int parts[NUM_PARTS]; // Array to hold the number of parts for each file
} FileTracker;

FileTracker fileTrackers[MAX_FILES];
int fileCount = 0;


ServerConfig serverConfigs[NUM_SERVERS];
int serverCount = 0;

int send_to_server_put(int server_index, Packet);
void read_config(const char *filename);
int connect_to_server(char *ip, int port);
int send_to_server_put(int server_index, Packet packet);
void execute_command(const char *cmd, char *filename);
void execute_put_command(const char *filename); 
void execute_get_command(const char *filename); 
void receive_chunks(int sock, const char *filename);
void add_chunk(const char *chunnk);
void check_completness(void);

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
    return sock;
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

void receive_chunks(int sock, const char *filename) {
    FILE *fp = NULL;
    char filepath[256];
    Packet packet;
    
    while (1) {
        ssize_t received = recv(sock, &packet, sizeof(packet), MSG_WAITALL);
        if (received <= 0) {
            if (received == 0)
                printf("Server closed the connection.\n");
            else
                perror("Receive failed");
            break;
        }

        if (strcmp(packet.data, "EOF") == 0) {
            printf("Complete  file part received.\n");
            if (fp) {
                fclose(fp);
                fp = NULL;
            }
            return;
        }

        if (!fp || strcmp(filepath, packet.filename) != 0) {
            if (fp) {
                fclose(fp);
                fp = NULL;
            }
            snprintf(filepath, sizeof(filepath), "%s", packet.filename);
            printf("Receiving file: %s\n", filepath);
            fp = fopen(filepath, "wb");
            if (!fp) {
                perror("Failed to open file for writing");
                break;
            }
        }

        fwrite(packet.data, 1, packet.data_size, fp);
    }

    if (fp) {
        fclose(fp);
    }
    return;
}


void execute_get_command(const char *filename) {
    printf("Requesting file: %s\n", filename);
    int sock = connect_to_server(serverConfigs[0].ip, serverConfigs[0].port);
        if (sock < 0) {
        perror("Error connecting to server");
        return;
    }

    Packet packet = {0};
    sprintf(packet.command, "GET");
    sprintf(packet.filename, "%s", filename);

    if(send(sock, &packet, sizeof(Packet), 0) < 0){
        perror("Failed to send GET command");
        close(sock);
        return;
    }
    
    for(int i = 0; i < NUM_SERVERS; i++){
        for(int j = 0; j < 2; j++){
            //Recive Chunks from server
            receive_chunks(sock, filename);
        }
    }
    close(sock);

    //Check 4 parts and Reconstruct File
    int fourPartsCounter = 0;
    for(int i = 0; i < 4; i++){
        char part[100];
        sprintf(part, "%s_%d", filename, i + 1);
        printf("Part: %s\n", part);
        if(part == "%s_%d", filename, i + 1){
            fourPartsCounter++; 
        }
    }

    if(fourPartsCounter == 4){
        printf("All 4 parts have been received\n");
        printf("Reconstructing file...\n");
        char final_path[256];
        sprintf(final_path, "%s_complete", filename);

        FILE *final_file = fopen(final_path, "wb");
        for(int i = 0; i < NUM_PARTS; i++){
            char part_path[256];
            sprintf(part_path, "%s_%d", filename, i + 1);
            FILE *part_file = fopen(part_path, "rb");

            char buffer[BUFFER_SIZE];
            size_t bytes;

            while((bytes = fread(buffer, 1, BUFFER_SIZE, part_file)) > 0){
                fwrite(buffer, 1, bytes, final_file);
            }
            fclose(part_file);
            //remove(part_path);  
        }
        fclose(final_file);
        printf("File %s has been reconstructed\n", filename);
    }
    else if(fourPartsCounter != 4){
        printf("%s is incomplete\n", filename);
    }
}

void add_chunk(const char *chunk) {
    char file[100];
    int part;
    sscanf(chunk, "%[^_]_%d", file, &part);  // Parse the filename and part number
    printf("File: %s, Part: %d\n", file, part);

    // Check if this file is already tracked
    for (int i = 0; i < fileCount; i++) {
        if (strcmp(fileTrackers[i].filename, file) == 0) {
            fileTrackers[i].parts[part - 1] = 1;  // Mark this part as received
            return;
        }
    }

    // New file, add to trackers
    if (fileCount < MAX_FILES) {
        strcpy(fileTrackers[fileCount].filename, file);
        memset(fileTrackers[fileCount].parts, 0, sizeof(fileTrackers[fileCount].parts));
        fileTrackers[fileCount].parts[part - 1] = 1;
        fileCount++;
    }
}

void check_completeness() {
    printf("Checking completeness...\n");
    printf("------------------------\n");
    for (int i = 0; i < fileCount; i++) {
        int complete = 1;
        for (int j = 0; j < NUM_PARTS; j++) {
            if (fileTrackers[i].parts[j] == 0) {
                complete = 0;
                break;
            }
        }

        if (complete) {
            printf("%s: COMPLETE\n", fileTrackers[i].filename);
        } else {
            printf("%s: INCOMPLETE\n", fileTrackers[i].filename);
        }
    }
}

void execute_list_command() {
    int sock = connect_to_server(serverConfigs[0].ip, serverConfigs[0].port);
    if (sock < 0) {
        perror("Error connecting to server");
        return;
    }

    Packet packet;
    memset(&packet, 0, sizeof(Packet));
    sprintf(packet.command, "LIST");
    if (send(sock, &packet, sizeof(Packet), 0) < 0) {
        perror("Failed to send LIST command");
        close(sock);
        return;
    }

    char buffer[BUFFER_SIZE];
    int total_bytes_received = 0;
    while(1) {
        int bytes_received = recv(sock, buffer + total_bytes_received, BUFFER_SIZE - total_bytes_received, 0);
        if (bytes_received <= 0) {
            if (bytes_received == 0) {
                printf("Server closed the connection.\n");
            } else {
                perror("Recv failed");
            }
            break;
        }
        total_bytes_received += bytes_received;
        buffer[total_bytes_received] = '\0'; // Ensure null-termination

        // Check for newline character indicating end of a chunk
        char *newline_pos;
        while ((newline_pos = strchr(buffer, '\n')) != NULL) {
            *newline_pos = '\0'; // Replace newline with null to end string
            printf("Received chunk: %s\n", buffer);
            add_chunk(buffer);
            // Move remaining data to the beginning of the buffer
            int remaining_data = total_bytes_received - (newline_pos - buffer + 1);
            memmove(buffer, newline_pos + 1, remaining_data);
            total_bytes_received = remaining_data;
            buffer[total_bytes_received] = '\0';
        }

        // Handle buffer overflow condition
        if (total_bytes_received >= BUFFER_SIZE - 1) {
            printf("Buffer overflow risk, resetting buffer.\n");
            total_bytes_received = 0; // Reset buffer if it's too full
        }
        check_completeness();
    }

    close(sock);

}


void execute_command(const char *cmd, char *filename) {
    for (int i = 0; i < serverCount; i++) {
        if (strcmp(cmd, "PUT") == 0 && filename != NULL) {
            printf("Executing PUT command\n");
            execute_put_command(filename);
        }
        if(strcmp(cmd, "GET") == 0 && filename != NULL) {
            printf("Executing GET command\n");
            execute_get_command(filename);
        }
        if(strcmp(cmd, "LIST") == 0) {
            printf("Executing LIST command\n");
            execute_list_command();
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
