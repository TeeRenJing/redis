#pragma once

#include <string>
#include <vector>
#include <queue>
#include <unordered_map>
#include <memory>
#include <chrono>

#include "Store.hpp"

// Structure to represent a blocked client
struct BlockedClient
{
    int client_fd;
    std::vector<std::string> keys;
    std::chrono::steady_clock::time_point block_start;
    std::chrono::duration<double> timeout;
    bool is_indefinite;

    BlockedClient(int fd, std::vector<std::string> k, std::chrono::duration<double> t)
        : client_fd(fd), keys(std::move(k)),
          block_start(std::chrono::steady_clock::now()),
          timeout(t), is_indefinite(t.count() == 0.0) {}
};

// Manages blocking operations for BLPOP
class BlockingManager
{
private:
    // Map from key -> queue of blocked clients waiting for that key
    std::unordered_map<std::string, std::queue<int>> blocked_clients_;
    // Map from client_fd -> BlockedClient info
    std::unordered_map<int, std::unique_ptr<BlockedClient>> client_info_;

public:
    // Add a client to the blocking queue for specified keys
    void add_blocked_client(int client_fd, const std::vector<std::string> &keys, std::chrono::duration<double> timeout);

    // Remove a client from all blocking queues (e.g., on disconnect)
    void remove_blocked_client(int client_fd);

    // Try to unblock clients waiting for a specific key
    // Returns true if a client was unblocked
    bool try_unblock_clients_for_key(const std::string &key, Store &kv_store);

    // Check for timed out clients and send them timeout responses
    void check_timeouts();

    // Check if a client is currently blocked
    bool is_client_blocked(int client_fd) const;

    // Get statistics (for debugging/monitoring)
    size_t get_blocked_client_count() const;
    size_t get_blocked_keys_count() const;
};

// Global instance - you might want to make this part of your server class instead
extern BlockingManager g_blocking_manager;