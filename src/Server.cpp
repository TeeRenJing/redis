#include "Server.hpp"
#include "RESP.hpp"
#include <iostream>
#include <array>
#include <thread>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

void handle_client(int client_fd)
{
  static std::unordered_map<std::string, std::string> kv_store;
  std::array<char, buffer_size> buffer;
  std::cout << "Client connected\n";
  while (true)
  {
    ssize_t bytes_received = recv(client_fd, buffer.data(), buffer.size(), 0);
    if (bytes_received <= 0)
      break;

    std::string_view request(buffer.data(), bytes_received);
    std::cout << "Received raw request: " << request << std::endl;

    auto parts = parse_resp(request);
    if (parts.empty())
    {
      // Fallback: treat as a single command line
      size_t start = request.find_first_not_of(" \r\n");
      size_t end = request.find_last_not_of(" \r\n");
      if (start == std::string::npos || end == std::string::npos)
      {
        send(client_fd, pong_response, strlen(pong_response), 0);
        continue;
      }
      std::string_view trimmed = request.substr(start, end - start + 1);
      size_t space_pos = trimmed.find(' ');
      parts.emplace_back(trimmed.substr(0, space_pos));
      if (space_pos != std::string_view::npos)
        parts.emplace_back(trimmed.substr(space_pos + 1));
    }

    if (!parts.empty())
    {
      std::string cmd(parts[0]);
      for (auto &c : cmd)
        c = std::toupper(c);
      std::cout << "Received command: " << cmd << std::endl;

      if (cmd == "PING")
      {
        send(client_fd, pong_response, strlen(pong_response), 0);
      }
      else if (cmd == "ECHO" && parts.size() > 1)
      {
        const auto &echo_arg = parts[1];
        std::string response = "$" + std::to_string(echo_arg.size()) + "\r\n" + std::string(echo_arg) + "\r\n";
        send(client_fd, response.c_str(), response.size(), 0);
      }
      else if (cmd == "SET" && parts.size() > 2)
      {
        kv_store[std::string(parts[1])] = std::string(parts[2]);
        constexpr const char *ok_response = "+OK\r\n";
        send(client_fd, ok_response, strlen(ok_response), 0);
      }
      else if (cmd == "GET" && parts.size() > 1)
      {
        auto it = kv_store.find(std::string(parts[1]));
        if (it != kv_store.end())
        {
          const std::string &val = it->second;
          std::string response = "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
          send(client_fd, response.c_str(), response.size(), 0);
        }
        else
        {
          constexpr const char *nil_response = "$-1\r\n";
          send(client_fd, nil_response, strlen(nil_response), 0);
        }
      }
      else
      {
        send(client_fd, pong_response, strlen(pong_response), 0);
      }
    }
    else
    {
      send(client_fd, pong_response, strlen(pong_response), 0);
    }
  }
  close(client_fd);
}

void start_server(int port)
{
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0)
  {
    std::cerr << "Failed to create server socket\n";
    return;
  }
  int reuse = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0)
  {
    std::cerr << "Failed to bind to port " << port << "\n";
    return;
  }
  constexpr int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0)
  {
    std::cerr << "listen failed\n";
    return;
  }
  struct sockaddr_in client_addr;
  const int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";
  std::cout << "Logs from your program will appear here!\n";
  while (true)
  {
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
    if (client_fd < 0)
    {
      std::cerr << "accept failed\n";
      continue;
    }
    std::thread(handle_client, client_fd).detach();
  }
  close(server_fd);
}

int main(int argc, char **argv)
{
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  start_server(6379);
  return 0;
}
