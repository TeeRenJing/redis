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
#include <algorithm>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <atomic>
#include "BlockingCommands.hpp"

class Server
{
public:
  Server(int port, int pool_size = 4)
      : port_(port), running_(true), pool_size_(pool_size) {}

  void run()
  {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
      std::cerr << "Failed to create server socket\n";
      return;
    }
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    if (bind(server_fd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr)) != 0)
    {
      std::cerr << "Failed to bind to port " << port_ << "\n";
      return;
    }
    constexpr int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0)
    {
      std::cerr << "listen failed\n";
      return;
    }
    sockaddr_in client_addr{};
    const int client_addr_len = sizeof(client_addr);
    std::cout << "Waiting for a client to connect...\n";
    std::cout << "Logs from your program will appear here!\n";

    // Start thread pool
    for (int i = 0; i < pool_size_; ++i)
      pool_.emplace_back(&Server::worker_thread, this);

    while (true)
    {
      int client_fd = accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), (socklen_t *)&client_addr_len);
      if (client_fd < 0)
      {
        std::cerr << "accept failed\n";
        continue;
      }
      {
        std::lock_guard lock(queue_mutex_);
        client_queue_.push(client_fd);
      }
      queue_cv_.notify_one();
    }

    running_ = false;
    queue_cv_.notify_all();
    for (auto &t : pool_)
      t.join();
    close(server_fd);
  }

private:
  void worker_thread()
  {
    while (running_)
    {
      int client_fd;
      {
        std::unique_lock lock(queue_mutex_);
        queue_cv_.wait(lock, [this]
                       { return !client_queue_.empty() || !running_; });
        if (!running_)
          break;
        client_fd = client_queue_.front();
        client_queue_.pop();
      }
      handle_client(client_fd);
    }
  }

  void handle_client(int client_fd)
  {
    std::array<char, 1024> buffer;
    std::cout << "Client connected\n";
    while (true)
    {
      ssize_t bytes_received = recv(client_fd, buffer.data(), buffer.size(), 0);
      if (bytes_received <= 0)
        break;

      std::string_view request(buffer.data(), bytes_received);

      // Log request as one-liner with escaped special chars
      std::string log_request;
      log_request.reserve(request.size());
      for (char c : request)
      {
        if (c == '\r')
          log_request += "\\r";
        else if (c == '\n')
          log_request += "\\n";
        else
          log_request += c;
      }
      std::cout << "Received raw request: " << log_request << std::endl;

      auto parts = parse_resp(request);
      if (parts.empty())
      {
        auto start = request.find_first_not_of(" \r\n");
        auto end = request.find_last_not_of(" \r\n");
        if (start == std::string_view::npos || end == std::string_view::npos)
        {
          handle_ping(client_fd);
          continue;
        }
        std::string_view trimmed = request.substr(start, end - start + 1);
        auto space_pos = trimmed.find(' ');
        parts.emplace_back(trimmed.substr(0, space_pos));
        if (space_pos != std::string_view::npos)
          parts.emplace_back(trimmed.substr(space_pos + 1));
      }

      if (!parts.empty())
      {
        std::string cmd(parts[0]);
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
        cmd.erase(std::remove_if(cmd.begin(), cmd.end(),
                                 [](char c)
                                 { return c == '\n' || c == '\r'; }),
                  cmd.end());
        std::cout << "Received command: " << cmd << std::endl;

        if (cmd == CMD_PING)
          handle_ping(client_fd);
        else if (cmd == CMD_ECHO)
          handle_echo(client_fd, parts);
        else if (cmd == CMD_SET)
          handle_set(client_fd, parts, kv_store_);
        else if (cmd == CMD_GET)
          handle_get(client_fd, parts, kv_store_);
        else if (cmd == CMD_LPUSH)
          handle_lpush(client_fd, parts, kv_store_);
        else if (cmd == CMD_RPUSH)
          handle_rpush(client_fd, parts, kv_store_);
        else if (cmd == CMD_LRANGE)
          handle_lrange(client_fd, parts, kv_store_);
        else if (cmd == CMD_LLEN)
          handle_llen(client_fd, parts, kv_store_);
        else if (cmd == CMD_LPOP)
          handle_lpop(client_fd, parts, kv_store_);
        else if (cmd == CMD_BLPOP)
          handle_blpop(client_fd, parts, kv_store_);
        // Add more command handlers as needed
        // For example, you might want to implement a command to delete keys, or to check if a key exists.
        // This modular approach allows for easy expansion of the command set without modifying existing code.
        // Each command handler can be implemented in a separate source file if desired, keeping the code organized and maintainable.
        else
          send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
      }
      else
      {
        handle_ping(client_fd);
      }
    }
    close(client_fd);
  }

  int port_;
  int pool_size_;
  std::vector<std::thread> pool_;
  std::queue<int> client_queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::atomic<bool> running_;
  Store kv_store_;
};

int main(int argc, char **argv)
{
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  Server server(6379);
  server.run();
  return 0;
}
