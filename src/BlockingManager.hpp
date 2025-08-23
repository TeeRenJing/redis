#pragma once

#include <unordered_map>
#include <queue>
#include <vector>
#include <string>
#include <memory>
#include <chrono>
#include <functional>
#include "Store.hpp"

struct BlockedClient
{
    int client_fd;
    std::vector<std::string> keys;
    std::chrono::duration<double> timeout;
    std::chrono::steady_clock::time_point block_start;
    bool is_indefinite;

    BlockedClient(int fd, const std::vector<std::string> &k, std::chrono::duration<double> t)
        : client_fd(fd), keys(k), timeout(t), block_start(std::chrono::steady_clock::now()), is_indefinite(false) {}
};

class BlockingManager
{
public:
    BlockingManager() = default;
    ~BlockingManager() = default;

    // Non-copyable, non-movable (singleton-like usage)
    BlockingManager(const BlockingManager &) = delete;
    BlockingManager &operator=(const BlockingManager &) = delete;
    BlockingManager(BlockingManager &&) = delete;
    BlockingManager &operator=(BlockingManager &&) = delete;

    /**
     * Add a client to the blocking manager for the given keys
     * @param client_fd The client file descriptor
     * @param keys List of keys the client is waiting for
     * @param timeout Timeout duration for the blocking operation
     */
    void add_blocked_client(int client_fd, const std::vector<std::string> &keys,
                            std::chrono::duration<double> timeout);

    void add_indefinitely_blocked_client(int client_fd, const std::vector<std::string> &keys);

    /**
     * Remove a blocked client from all queues
     * @param client_fd The client file descriptor to remove
     */
    void remove_blocked_client(int client_fd);

    /**
     * Try to unblock clients waiting for a specific key
     * @param key The key that now has available data
     * @param kv_store Reference to the key-value store
     * @param send_callback Callback function to send responses to clients
     * @return true if a client was unblocked, false otherwise
     */
    bool try_unblock_clients_for_key(const std::string &key, Store &kv_store,
                                     std::function<void(int, const std::string &)> send_callback);

    /**
     * Check for timed-out clients and send them NIL responses
     * @param send_callback Callback function to send responses to clients
     */
    void check_timeouts(std::function<void(int, const std::string &)> send_callback);

    /**
     * Check if a key can immediately satisfy a blocking pop operation
     * @param key The key to check
     * @param kv_store Reference to the key-value store
     * @return true if the key has available elements, false otherwise
     */
    bool can_immediate_pop(const std::string &key, Store &kv_store) const;

    /**
     * Get all keys that a specific client is waiting for
     * @param client_fd The client file descriptor
     * @return Vector of keys the client is waiting for (empty if client not blocked)
     */
    std::vector<std::string> get_client_keys(int client_fd) const;

    /**
     * Check if a client is currently blocked
     * @param client_fd The client file descriptor to check
     * @return true if client is blocked, false otherwise
     */
    bool is_client_blocked(int client_fd) const;

    /**
     * Get the total number of blocked clients
     * @return Number of currently blocked clients
     */
    size_t get_blocked_client_count() const;

    /**
     * Get the number of keys that have blocked clients
     * @return Number of keys with waiting clients
     */
    size_t get_blocked_keys_count() const;

private:
    // Map from key name to queue of waiting client file descriptors
    std::unordered_map<std::string, std::queue<int>> blocked_clients_;

    // Map from client fd to their blocking information
    std::unordered_map<int, std::unique_ptr<BlockedClient>> client_info_;
};

// Global instance declaration
extern BlockingManager g_blocking_manager;