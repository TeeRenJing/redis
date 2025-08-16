#pragma once
#include <string>
#include <unordered_map>

constexpr int buffer_size = 1024;
constexpr const char *pong_response = "+PONG\r\n";

void start_server(int port);
void handle_client(int client_fd);