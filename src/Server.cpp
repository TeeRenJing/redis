#include "Server.hpp"
#include "RESP.hpp"
#include "Commands.hpp"
#include "Store.hpp"
#include <iostream>
#include <array>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <unordered_map>

void handle_client(int client_fd)
{
  static Store kv_store;
  std::array<char, 1024> buffer;
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
      size_t start = request.find_first_not_of(" \r\n");
      size_t end = request.find_last_not_of(" \r\n");
      if (start == std::string::npos || end == std::string::npos)
      {
        handle_ping(client_fd);
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

      static const std::unordered_map<std::string, std::function<void(int, const std::vector<std::string_view> &, Store &)>> handlers = {
          {CMD_PING, handle_ping},
          {CMD_ECHO, handle_echo},
          {CMD_SET, handle_set},
          {CMD_GET, handle_get},
          {CMD_RPUSH, handle_rpush}};

      auto it = handlers.find(cmd);
      if (it != handlers.end())
      {
        it->second(client_fd, parts, kv_store);
      }
      else
      {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
      }
    }
    else
    {
      handle_ping(client_fd);
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
