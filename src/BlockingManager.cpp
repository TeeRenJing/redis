#include "BlockingManager.hpp"
#include "Commands.hpp" // For Store, ListValue, RESP constants
#include <sys/socket.h>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <format>

// Global instance
BlockingManager g_blocking_manager;

BlockedClient::BlockedClient(int fd, std::vector<std::string> k, std::chrono::seconds t)
    : client_fd(fd), keys(std::move(k)),
      block_start(std::chrono::steady_clock::now()),
      timeout(t), is_indefinite(t.count() == 0)
{
}

void BlockingManager::add_blocked_client(int client_fd, const std::vector<std::string> &keys, std::chrono::seconds timeout)
{
    // Create blocked client info
    client_info_[client_fd] = std::make_unique<BlockedClient>(client_fd, keys, timeout);

    // Add client to waiting queue for each key
    for (const auto &key : keys)
    {
        blocked_clients_[key].push(client_fd);
    }

    std::cout << "Client " << client_fd << " blocked on keys: ";
    for (const auto &key : keys)
    {
        std::cout << key << " ";
    }
    std::cout << "(timeout: " << timeout.count() << "s)" << std::endl;
}

void BlockingManager::remove_blocked_client(int client_fd)
{
    auto it = client_info_.find(client_fd);
    if (it == client_info_.end())
        return;

    std::cout << "Removing blocked client " << client_fd << std::endl;

    // Remove from all key queues
    // Note: This is O(n*m) where n=keys, m=avg queue size
    // For high-performance servers, consider using a more sophisticated data structure
    for (auto &[key, queue] : blocked_clients_)
    {
        std::queue<int> temp_queue;
        while (!queue.empty())
        {
            int fd = queue.front();
            queue.pop();
            if (fd != client_fd)
            {
                temp_queue.push(fd);
            }
        }
        queue = std::move(temp_queue);
    }

    // Clean up empty queues to avoid memory bloat
    for (auto it = blocked_clients_.begin(); it != blocked_clients_.end();)
    {
        if (it->second.empty())
        {
            it = blocked_clients_.erase(it);
        }
        else
        {
            ++it;
        }
    }

    client_info_.erase(it);
}

bool BlockingManager::try_unblock_clients_for_key(const std::string &key, Store &kv_store)
{
    auto queue_it = blocked_clients_.find(key);
    if (queue_it == blocked_clients_.end() || queue_it->second.empty())
    {
        return false;
    }

    // Find the list in the store
    auto store_it = kv_store.find(key);
    if (store_it == kv_store.end())
        return false;

    auto *lval = dynamic_cast<ListValue *>(store_it->second.get());
    if (!lval || lval->values.empty())
        return false;

    // Get the longest-waiting client (FIFO order)
    int client_fd = queue_it->second.front();
    queue_it->second.pop();

    // Verify client is still in our records
    auto client_it = client_info_.find(client_fd);
    if (client_it == client_info_.end())
    {
        // Client was removed but queue wasn't cleaned up properly
        return try_unblock_clients_for_key(key, kv_store); // Try next client
    }

    // Pop element from list
    std::string element = std::move(lval->values.front());
    lval->values.erase(lval->values.begin());

    // If list is empty, remove it from store
    if (lval->values.empty())
    {
        kv_store.erase(store_it);
    }

    // Send response: array with [key, element]
    std::string response = std::format("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                       key.size(), key,
                                       element.size(), element);

    ssize_t sent = send(client_fd, response.c_str(), response.size(), 0);
    if (sent < 0)
    {
        std::cerr << "Failed to send response to unblocked client " << client_fd << std::endl;
    }
    else
    {
        std::cout << "Unblocked client " << client_fd << " with element from key: " << key << std::endl;
    }

    // Remove client from all queues (they're no longer blocked)
    remove_blocked_client(client_fd);

    return true;
}

void BlockingManager::check_timeouts()
{
    auto now = std::chrono::steady_clock::now();

    std::vector<int> timed_out_clients;

    // Find all timed out clients
    for (const auto &[client_fd, client_info] : client_info_)
    {
        if (!client_info->is_indefinite)
        {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - client_info->block_start);
            if (elapsed >= client_info->timeout)
            {
                timed_out_clients.push_back(client_fd);
            }
        }
    }

    // Send timeout responses and remove clients
    for (int client_fd : timed_out_clients)
    {
        std::cout << "Client " << client_fd << " timed out on BLPOP" << std::endl;

        ssize_t sent = send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        if (sent < 0)
        {
            std::cerr << "Failed to send timeout response to client " << client_fd << std::endl;
        }

        remove_blocked_client(client_fd);
    }
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