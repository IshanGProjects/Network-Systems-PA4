#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <regex.h>
#include <openssl/md5.h>

#define BUFFER_SIZE 1024

void *handle_client(void *socket_desc);
void setup_directory(const char *dir);
void process_put(int sock, char *filename, char *data, int data_size, int num_servers);
void process_get(int sock, char *filename);
void process_list(int sock);
char *get_full_path(const char *dir, const char *filename);

int num_servers = 4;

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <directory> <port>\n", argv[0]);
        exit(1);
    }

    const char* directory = argv[1];
    int port = atoi(argv[2]);

    setup_directory(directory);
    printf("Directory setup complete. Listening on port %d\n", port);

    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    socklen_t addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // Set SO_REUSEADDR
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR");
        exit(EXIT_FAILURE);
    }

    // Set SO_REUSEPORT separately, only if necessary and available
    #ifdef SO_REUSEPORT
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEPORT");
        exit(EXIT_FAILURE);
    }
    #endif

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf("Server is now listening...\n");

    while (1) {
        int *new_sock = malloc(sizeof(int));
        new_socket = accept(server_fd, (struct sockaddr *)&address, &addrlen);
        if (new_socket < 0) {
            perror("accept");
            continue;
        }
        
        int* client_sock = malloc(sizeof(int));
        if (client_sock) {
            *client_sock = new_socket;
            pthread_t thread_id;
            if (pthread_create(&thread_id, NULL, handle_client, client_sock) != 0) {
                perror("Failed to create thread");
                free(client_sock); // Free memory if thread creation fails
            } else {
                pthread_detach(thread_id);
                printf("Client connected: thread %p started\n", (void*)thread_id);
            }
        } else {
            perror("Failed to allocate memory for socket descriptor");
            close(new_socket); // Close the socket if we can't allocate memory
        }
    }

    return 0;
}

void setup_directory(const char *dir) {
    struct stat st = {0};

    if (stat(dir, &st) == -1) {
        if (mkdir(dir, 0700) != 0) {
            perror("Failed to create directory");
            exit(EXIT_FAILURE);
        }
    }
}

void *handle_client(void *socket_desc) {
    int sock = *(int*)socket_desc;
    free(socket_desc);  // Free the memory allocated for the socket descriptor
    
    char buffer[BUFFER_SIZE];
    int read_size;

    while ((read_size = recv(sock, buffer, BUFFER_SIZE - 1, 0)) > 0) {
        buffer[read_size] = '\0';  // Ensure the buffer is null-terminated

        // Using strstr to locate the command and separate it by finding the first space
        char *command = strtok(buffer, " ");
        if (!command) {
            printf("Invalid command format.\n");
            continue;
        }

        if (strcmp(command, "PUT") == 0) {
            // Extracting the filename by finding the first quote
            char *filename = strtok(NULL, "\"");
            if (!filename) {
                printf("Invalid PUT command: No filename provided.\n");
                continue;
            }

            // Moving the pointer to the start of the actual data after the filename and the space after the quote
            char *data = strtok(NULL, "\"");
            if (!data) {
                printf("Invalid PUT command: No data found.\n");
                continue;
            }

            int data_size = strlen(data);
            printf("Received PUT command for file %s with data size %d\n", filename, data_size);
            process_put(sock, filename, data, data_size, num_servers);
            printf("PUT command processed for file %s\n", filename);
        } else if (strcmp(command, "GET") == 0) {
            char *filename = strtok(NULL, " ");
            if (!filename) {
                printf("Invalid GET command: No filename provided.\n");
                continue;
            }
            process_get(sock, filename);
            printf("GET command processed for file %s\n", filename);
        } else if (strcmp(command, "LIST") == 0) {
            process_list(sock);
            printf("LIST command processed\n");
        } else {
            printf("Received unknown command: %s\n", buffer);
        }
    }

    if (read_size == 0) {
        printf("Client disconnected.\n");
    } else if (read_size == -1) {
        perror("recv failed");
    }

    close(sock);
    return NULL;
}


void process_put(int sock, char *filename, char *data, int data_size, int num_servers) {
    printf("Processing PUT for file: %s\n", filename);
    printf("Data to be split: %s\n", data);
    printf("Data size: %d\n", data_size);
    unsigned char hash[MD5_DIGEST_LENGTH];
    MD5((unsigned char *)filename, strlen(filename), hash);
    int x = hash[0] % num_servers;  // Simple hash function based on MD5

    // Define chunk distribution based on the hash index x
    int server_pairs[4][4][2] = {
        {{0, 1}, {1, 2}, {2, 3}, {3, 0}},  // x = 0
        {{3, 0}, {0, 1}, {1, 2}, {2, 3}},  // x = 1
        {{2, 3}, {3, 0}, {0, 1}, {1, 2}},  // x = 2
        {{1, 2}, {2, 3}, {3, 0}, {0, 1}}   // x = 3
    };

    int chunk_size = data_size / 4;
    printf("Chunk size: %d\n", chunk_size);
    char *chunks[4];
    for (int i = 0; i < 4; i++) {
        chunks[i] = malloc(chunk_size + (i == 3 ? data_size % 4 : 0));  // Allocate for last chunk with remainder
        memcpy(chunks[i], data + i * chunk_size, chunk_size + (i == 3 ? data_size % 4 : 0));
    }

    // Create directories and write chunks to respective files
    for (int i = 0; i < 4; i++) {
        char dir_path[1024];
        snprintf(dir_path, sizeof(dir_path), "./dfs%d/%s_files", i + 1, filename);
        mkdir(dir_path, 0777);  // Ensure the directory exists

        int chunk_index1 = server_pairs[x][i][0];
        int chunk_index2 = server_pairs[x][i][1];
        char path_buffer1[1024], path_buffer2[1024];
        snprintf(path_buffer1, sizeof(path_buffer1), "%s/part%d", dir_path, chunk_index1 + 1);
        snprintf(path_buffer2, sizeof(path_buffer2), "%s/part%d", dir_path, chunk_index2 + 1);

        FILE *file1 = fopen(path_buffer1, "wb");
        FILE *file2 = fopen(path_buffer2, "wb");
        if (file1 && file2) {
            fwrite(chunks[chunk_index1], sizeof(char), chunk_size, file1);
            fwrite(chunks[chunk_index2], sizeof(char), chunk_size, file2);
            fclose(file1);
            fclose(file2);
            printf("Chunks %d and %d written to %s and %s successfully.\n", chunk_index1 + 1, chunk_index2 + 1, path_buffer1, path_buffer2);
        } else {
            perror("Failed to open file for writing");
        }
    }

    // Free allocated memory for chunks
    for (int i = 0; i < 4; i++) {
        free(chunks[i]);
    }
}




void process_get(int sock, char *filename) {
    char *full_path = get_full_path("server_directory", filename);
    FILE *file = fopen(full_path, "rb");
    if (file != NULL) {
        char data[BUFFER_SIZE];
        size_t bytes_read;
        while ((bytes_read = fread(data, sizeof(char), BUFFER_SIZE, file)) > 0) {
            send(sock, data, bytes_read, 0);
        }
        fclose(file);
        printf("File %s sent successfully.\n", filename);
    } else {
        perror("Failed to open file for reading");
    }
    free(full_path);
}


void process_list(int sock) {
    DIR *d, *subdir;
    struct dirent *dir, *subdir_entry;
    regex_t regex;
    int ret;
    char msgbuf[100];
    char *base_directory = "."; // Current directory

    // Compile regex for directory names like 'dfs1', 'dfs2', ..., 'dfsn'
    ret = regcomp(&regex, "^dfs[0-9]+$", REG_EXTENDED);
    if (ret) {
        fprintf(stderr, "Could not compile regex\n");
        return;
    }

    d = opendir(base_directory);
    if (d) {
        while ((dir = readdir(d)) != NULL) {
            if (dir->d_type == DT_DIR) { // Check if it is a directory
                ret = regexec(&regex, dir->d_name, 0, NULL, 0);
                if (!ret) {
                    // Directory name matches the pattern, open and list files
                    char subdir_path[1024];
                    snprintf(subdir_path, sizeof(subdir_path), "%s/%s", base_directory, dir->d_name);
                    subdir = opendir(subdir_path);
                    printf("Opening directory: %s\n", subdir_path);
                    if (subdir) {
                        while ((subdir_entry = readdir(subdir)) != NULL) {
                            if (subdir_entry->d_type == DT_REG) { // Check if it is a regular file
                                char filename_path[2048];
                                snprintf(filename_path, sizeof(filename_path), "%s/%s", subdir_path, subdir_entry->d_name);
                                printf("Sending filename: %s\n", filename_path);
                                send(sock, subdir_entry->d_name, strlen(subdir_entry->d_name), 0);
                                send(sock, "\n", 1, 0); // Send newline after each filename
                            }
                        }
                        closedir(subdir);
                    } else {
                        perror("Failed to open subdirectory");
                    }
                } else if (ret == REG_NOMATCH) {
                    continue; // No match, skip this directory
                } else {
                    regerror(ret, &regex, msgbuf, sizeof(msgbuf));
                    fprintf(stderr, "Regex match failed: %s\n", msgbuf);
                }
            }
        }
        closedir(d);
        printf("Directory listing sent successfully.\n");
    } else {
        perror("Failed to open base directory for listing");
    }

    regfree(&regex); // Free the compiled regular expression
}

char *get_full_path(const char *dir, const char *filename) {
    char *full_path = malloc(strlen(dir) + strlen(filename) + 2);
    sprintf(full_path, "%s/%s", dir, filename);
    return full_path;
}
