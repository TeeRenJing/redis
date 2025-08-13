#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <array>
#include <thread>
#include <vector>

constexpr int buffer_size = 1024;
constexpr const char *pong_response = "+PONG\r\n";

void handle_client(int client_fd)
{
  std::array<char, buffer_size> buffer;
  std::cout << "Client connected\n";
  while (true)
  {
    ssize_t bytes_received = recv(client_fd, buffer.data(), buffer.size(), 0);
    if (bytes_received <= 0)
      break; // Client disconnected or error

    std::string_view request(buffer.data(), bytes_received);
    std::cout << "Received raw request: " << request << std::endl;

    std::vector<std::string_view> parts;
    size_t pos = 0;

    if (!request.empty() && request[pos] == '*')
    {
      // RESP array parsing
      pos = request.find("\r\n", pos);
      if (pos == std::string::npos)
        continue;
      pos += 2;
      while (pos < request.size() && request[pos] == '$')
      {
        size_t len_end = request.find("\r\n", pos);
        if (len_end == std::string::npos)
          break;
        int len = std::stoi(std::string(request.substr(pos + 1, len_end - pos - 1)));
        pos = len_end + 2;
        if (pos + len > request.size())
          break;
        parts.emplace_back(request.substr(pos, len));
        pos += len + 2; // Skip over string and \r\n
      }
    }
    else
    {
      // Fallback: treat as a single command line
      size_t start = request.find_first_not_of(" \r\n");
      size_t end = request.find_last_not_of(" \r\n");
      if (start == std::string::npos || end == std::string::npos)
      {
        send(client_fd, pong_response, strlen(pong_response), 0);
        continue;
      }
      request = request.substr(start, end - start + 1);
      size_t space_pos = request.find(' ');
      parts.emplace_back(request.substr(0, space_pos));
      if (space_pos != std::string::npos)
        parts.emplace_back(request.substr(space_pos + 1));
    }

    // Command handling
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

int main(int argc, char **argv)
{
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0)
  {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
  {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0)
  {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }

  constexpr int connection_backlog = 5;

  if (listen(server_fd, connection_backlog) != 0)
  {
    std::cerr << "listen failed\n";
    return 1;
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
  return 0;
}
