#include "BlockingManager.hpp"
#include "Commands.hpp" // For Store, ListValue, RESP constants
#include <sys/socket.h>
#include <cstring>
#include <iostream>
#include <algorithm>

// Global instance of BlockingManager (Singleton-like behavior for simplicity)
BlockingManager g_blocking_manager;

void BlockingManager::add_blocked_client(
    int client_fd,
    const std::vector<std::string> &keys,
    std::chrono::duration<double> timeout)
{
    std::cout << "[ADD_BLOCKED LOG] === ENTERING add_blocked_client ===" << std::endl;
    std::cout << "[ADD_BLOCKED LOG] client_fd: " << client_fd << std::endl;

    // Log keys being watched
    for (size_t i = 0; i < keys.size(); ++i)
    {
        std::cout << "[ADD_BLOCKED LOG] keys[" << i << "]: " << keys[i] << std::endl;
    }

    // Convert timeout to milliseconds for better precision
    const std::chrono::milliseconds timeout_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(timeout);
    std::cout << "[ADD_BLOCKED LOG] timeout(ms): " << timeout_ms.count() << std::endl;

    // Defensive check: 0 or negative timeout means "block indefinitely"
    if (timeout_ms.count() <= 0)
    {
        std::cout << "[ADD_BLOCKED LOG] WARNING: timeout is <= 0 ("
                  << timeout_ms.count()
                  << "ms), delegating to add_indefinitely_blocked_client()" << std::endl;
        add_indefinitely_blocked_client(client_fd, keys);
        return;
    }

    bool is_indefinite = (timeout_ms.count() >= 315360000000LL); // 10 years in ms

    // Allocate new BlockedClient with RAII (unique_ptr ensures automatic cleanup)
    std::unique_ptr<BlockedClient> blocked_client =
        std::make_unique<BlockedClient>(client_fd, keys, timeout_ms);

    blocked_client->is_indefinite = is_indefinite;

    // Insert into client registry (O(1) average due to unordered_map)
    client_info_[client_fd] = std::move(blocked_client);

    // Add client_fd to each key's blocking queue
    for (const std::string &key : keys)
    {
        blocked_clients_[key].push(client_fd);
    }

    std::cout << "[ADD_BLOCKED LOG] Client " << client_fd
              << " successfully added to blocking manager with timeout "
              << timeout_ms.count() << "ms" << std::endl;
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
    std::unordered_map<int, std::unique_ptr<BlockedClient>>::iterator it =
        client_info_.find(client_fd);
    if (it == client_info_.end())
        return;

    std::cout << "[REMOVE LOG] Removing blocked client " << client_fd << std::endl;

    // Remove client_fd from all key queues (O(n*m) worst case but acceptable here)
    for (std::pair<const std::string, std::queue<int>> &pair : blocked_clients_)
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

    // Clean up empty queues
    for (auto iter = blocked_clients_.begin(); iter != blocked_clients_.end();)
    {
        if (iter->second.empty())
        {
            iter = blocked_clients_.erase(iter);
        }
        else
        {
            ++iter;
        }
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
        return false;

    // Pop the first waiting client (FIFO)
    int client_fd = queue_it->second.front();
    queue_it->second.pop();

    // Safety: Ensure client is still registered
    if (client_info_.find(client_fd) == client_info_.end())
        return try_unblock_clients_for_key(key, kv_store, send_callback);

    // Pop element from list
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

void BlockingManager::check_timeouts(
    std::function<void(int, const std::string &)> send_callback)
{
    std::cout << "[TIMEOUT LOG] check_timeouts() called" << std::endl;

    // Explicitly declare type to avoid accidental floating-point rounding
    const std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    const long long now_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch())
            .count();

    std::cout << "[TIMEOUT LOG] Current time (ms since epoch): " << now_ms << std::endl;

    std::vector<int> timed_out_clients;

    for (const std::pair<const int, std::unique_ptr<BlockedClient>> &entry : client_info_)
    {
        int client_fd = entry.first;
        const std::unique_ptr<BlockedClient> &blocked_client_ptr = entry.second;

        if (blocked_client_ptr->is_indefinite)
        {
            std::cout << "[TIMEOUT LOG] Skipping indefinite client: " << client_fd << std::endl;
            continue;
        }

        // Calculate expiration point using the same unit (milliseconds)
        const std::chrono::steady_clock::time_point timeout_time =
            blocked_client_ptr->block_start + blocked_client_ptr->timeout;

        if (now >= timeout_time)
        {
            std::cout << "[TIMEOUT LOG] Client " << client_fd
                      << " timed out (expired)" << std::endl;
            timed_out_clients.push_back(client_fd);
        }
        else
        {
            const std::chrono::milliseconds remaining_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    timeout_time - now);
            std::cout << "[TIMEOUT LOG] Client " << client_fd
                      << " has " << remaining_ms.count()
                      << "ms remaining" << std::endl;
        }
    }

    if (timed_out_clients.empty())
    {
        std::cout << "[TIMEOUT LOG] No clients have timed out" << std::endl;
        return;
    }

    // Send NIL response to each timed-out client
    for (int client_fd : timed_out_clients)
    {
        const std::string nil_response = "*-1\r\n"; // RESP NIL array
        send_callback(client_fd, nil_response);

        std::cout << "[TIMEOUT LOG] Sent NIL response to client "
                  << client_fd << " and removing" << std::endl;

        remove_blocked_client(client_fd);
    }
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
