#include "BlockingManager.hpp"
#include "Commands.hpp" // For Store, ListValue, RESP constants
#include <sys/socket.h>
#include <cstring>
#include <iostream>
#include <algorithm>

// Global instance of BlockingManager (Singleton-like behavior for simplicity)
// Good Practice: Avoid true singletons where possible, but for simplicity, this acts as one.
BlockingManager g_blocking_manager;

void BlockingManager::add_blocked_client(
    int client_fd,
    const std::vector<std::string> &keys,
    std::chrono::duration<double> timeout)
{
    std::cout << "[DEBUG] ENTER add_blocked_client for client_fd: " << client_fd << std::endl;
    std::cout << "[DEBUG] Keys to block on: ";
    for (const auto &key : keys)
        std::cout << key << " ";
    std::cout << std::endl;

    // Convert generic duration to milliseconds for easier comparison
    const std::chrono::milliseconds timeout_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(timeout);
    std::cout << "[DEBUG] Timeout passed (ms): " << timeout_ms.count() << std::endl;

    // Concept: Handling zero/negative timeout as indefinite wait
    if (timeout_ms.count() <= 0)
    {
        std::cout << "[DEBUG] Timeout <= 0, using indefinite block" << std::endl;
        add_indefinitely_blocked_client(client_fd, keys);
        return;
    }

    // Large timeout threshold = "indefinite" blocking
    bool is_indefinite = (timeout_ms.count() >= 315360000000LL); // 10 years

    // RAII: use unique_ptr to avoid manual memory management
    auto blocked_client = std::make_unique<BlockedClient>(client_fd, keys, timeout_ms);
    blocked_client->is_indefinite = is_indefinite;

    // Record start time using steady_clock (monotonic clock → not affected by system time changes)
    blocked_client->block_start = std::chrono::steady_clock::now();
    std::cout << "[DEBUG] block_start (ms since epoch): "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     blocked_client->block_start.time_since_epoch())
                     .count()
              << std::endl;

    // Move ownership of blocked_client into the map
    client_info_[client_fd] = std::move(blocked_client);

    // Map keys → waiting client queue
    for (const auto &key : keys)
        blocked_clients_[key].push(client_fd);

    std::cout << "[DEBUG] Client " << client_fd
              << " added successfully with timeout " << timeout_ms.count() << "ms" << std::endl;
}

void BlockingManager::check_timeouts(
    std::function<void(int, const std::string &)> send_callback)
{
    const auto now = std::chrono::steady_clock::now();
    const long long now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 now.time_since_epoch())
                                 .count();

    std::vector<int> timed_out_clients;

    for (const auto &entry : client_info_)
    {
        int client_fd = entry.first;
        const auto &client_ptr = entry.second;

        if (client_ptr->is_indefinite)
        {
            std::cout << "[DEBUG] Skipping indefinite client " << client_fd << std::endl;
            continue;
        }

        const auto timeout_time = client_ptr->block_start + client_ptr->timeout;
        const long long timeout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         timeout_time.time_since_epoch())
                                         .count();

        if (now >= timeout_time)
        {
            std::cout << "[TIMEOUT LOG] Client " << client_fd << " has TIMED OUT" << std::endl;
            timed_out_clients.push_back(client_fd);
        }
        else
        {
            const auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                          timeout_time - now)
                                          .count();
            std::cout << "[DEBUG] Client " << client_fd
                      << " remaining time: " << remaining_ms << "ms" << std::endl;
        }
    }

    for (int client_fd : timed_out_clients)
    {
        const std::string nil_response = "*-1\r\n"; // RESP NIL
        std::cout << "[NIL SEND LOG] Sending NIL due to timeout for client_fd=" << client_fd << std::endl;
        send_callback(client_fd, nil_response);
        remove_blocked_client(client_fd);
    }
}

void BlockingManager::add_indefinitely_blocked_client(
    int client_fd, const std::vector<std::string> &keys)
{
    std::cout << "[ADD_INDEFINITE_BLOCKED LOG] Adding client with indefinite timeout"
              << std::endl;

    // Represent "indefinite" as 10 years (arbitrarily large)
    constexpr std::chrono::milliseconds INDEFINITE_TIMEOUT_MS(315360000000LL);
    add_blocked_client(client_fd, keys, INDEFINITE_TIMEOUT_MS);
}

void BlockingManager::remove_blocked_client(int client_fd)
{
    auto it = client_info_.find(client_fd);
    if (it == client_info_.end())
        return;

    std::cout << "[REMOVE LOG] Removing blocked client " << client_fd << std::endl;

    // Complexity note: O(n*m) worst case if many keys with same client
    for (auto &pair : blocked_clients_)
    {
        std::queue<int> temp_queue;
        while (!pair.second.empty())
        {
            int fd = pair.second.front();
            pair.second.pop();
            if (fd != client_fd)
            {
                temp_queue.push(fd);
            }
        }
        pair.second = std::move(temp_queue);
    }

    // Remove empty key queues (avoid memory bloat)
    for (auto iter = blocked_clients_.begin(); iter != blocked_clients_.end();)
    {
        if (iter->second.empty())
            iter = blocked_clients_.erase(iter);
        else
            ++iter;
    }

    client_info_.erase(it);
}

bool BlockingManager::try_unblock_clients_for_key(
    const std::string &key,
    Store &kv_store,
    std::function<void(int, const std::string &)> send_callback)
{
    std::cout << "[UNBLOCK LOG] Checking for blocked clients on key: " << key << std::endl;

    auto queue_it = blocked_clients_.find(key);
    if (queue_it == blocked_clients_.end() || queue_it->second.empty())
        return false;

    auto store_it = kv_store.find(key);
    if (store_it == kv_store.end())
        return false;

    ListValue *list_value = dynamic_cast<ListValue *>(store_it->second.get());
    if (!list_value || list_value->values.empty())
    {
        std::cout << "[NIL SEND LOG] Key exists but list is empty for client(s) on key=" << key << std::endl;
        return false;
    }

    // FIFO: pop the first waiting client
    int client_fd = queue_it->second.front();
    queue_it->second.pop();

    // Safety: Ensure client is still registered
    if (client_info_.find(client_fd) == client_info_.end())
    {
        std::cout << "[UNBLOCK LOG] Client no longer registered, retrying for key=" << key << std::endl;
        return try_unblock_clients_for_key(key, kv_store, send_callback);
    }

    // Pop element from list (front of vector = O(n) removal)
    std::string element = std::move(list_value->values.front());
    list_value->values.erase(list_value->values.begin());

    if (list_value->values.empty())
        kv_store.erase(store_it);

    // RESP response: [key, element]
    std::string response = "*2\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n" +
                           "$" + std::to_string(element.size()) + "\r\n" + element + "\r\n";
    send_callback(client_fd, response);

    std::cout << "[UNBLOCK LOG] Unblocked client " << client_fd
              << " with element: " << element << std::endl;

    remove_blocked_client(client_fd);
    return true;
}

// Utility helpers remain unchanged
bool BlockingManager::can_immediate_pop(const std::string &key, Store &kv_store) const
{
    auto store_iter = kv_store.find(key);
    if (store_iter == kv_store.end())
        return false;

    const ListValue *list_value = dynamic_cast<ListValue *>(store_iter->second.get());
    return list_value && !list_value->values.empty();
}

std::vector<std::string> BlockingManager::get_client_keys(int client_fd) const
{
    auto it = client_info_.find(client_fd);
    if (it != client_info_.end())
    {
        return it->second->keys;
    }
    return {};
}

bool BlockingManager::is_client_blocked(int client_fd) const
{
    return client_info_.find(client_fd) != client_info_.end();
}

size_t BlockingManager::get_blocked_client_count() const
{
    return client_info_.size();
}

size_t BlockingManager::get_blocked_keys_count() const
{
    return blocked_clients_.size();
}
