#include "BlockingManager.hpp"
#include "Commands.hpp" // For Store, ListValue, RESP constants
#include <sys/socket.h>
#include <cstring>
#include <iostream>
#include <algorithm>

// Global instance
BlockingManager g_blocking_manager;

void BlockingManager::add_blocked_client(int client_fd, const std::vector<std::string> &keys, std::chrono::duration<double> timeout)
{
    std::cout << "[ADD_BLOCKED LOG] Adding client " << client_fd << " with timeout " << timeout.count() << " seconds" << std::endl;

    // Determine if this is indefinite blocking
    bool indefinite = (timeout.count() >= 315360000); // Our "infinite" timeout value

    // Create BlockedClient info
    auto blocked_client = std::make_unique<BlockedClient>(client_fd, keys, timeout);
    blocked_client->is_indefinite = indefinite;

    std::cout << "[ADD_BLOCKED LOG] Client " << client_fd << " indefinite: " << (indefinite ? "YES" : "NO") << std::endl;

    // Store client info
    client_info_[client_fd] = std::move(blocked_client);

    // Add client to each key's waiting queue
    for (const auto &key : keys)
    {
        blocked_clients_[key].push(client_fd);
        std::cout << "[ADD_BLOCKED LOG] Added client " << client_fd << " to queue for key: " << key << std::endl;
    }

    std::cout << "[ADD_BLOCKED LOG] Client " << client_fd << " successfully added to blocking manager" << std::endl;
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
    std::cout << "[UNBLOCK LOG] try_unblock_clients_for_key called for key: " << key << std::endl;

    auto queue_it = blocked_clients_.find(key);
    if (queue_it == blocked_clients_.end() || queue_it->second.empty())
    {
        std::cout << "[UNBLOCK LOG] No blocked clients for key: " << key << std::endl;
        return false;
    }

    std::cout << "[UNBLOCK LOG] Found " << queue_it->second.size() << " blocked clients for key: " << key << std::endl;

    // Find the list in the store
    auto store_it = kv_store.find(key);
    if (store_it == kv_store.end())
    {
        std::cout << "[UNBLOCK LOG] Key not found in store: " << key << std::endl;
        return false;
    }

    auto *lval = dynamic_cast<ListValue *>(store_it->second.get());
    if (!lval || lval->values.empty())
    {
        std::cout << "[UNBLOCK LOG] Key exists but list is empty or not a list: " << key << std::endl;
        return false;
    }

    std::cout << "[UNBLOCK LOG] List has " << lval->values.size() << " elements" << std::endl;

    // Get the longest-waiting client (FIFO order)
    int client_fd = queue_it->second.front();
    queue_it->second.pop();

    std::cout << "[UNBLOCK LOG] Attempting to unblock client: " << client_fd << std::endl;

    // Verify client is still in our records
    auto client_it = client_info_.find(client_fd);
    if (client_it == client_info_.end())
    {
        std::cout << "[UNBLOCK LOG] Client " << client_fd << " not found in client_info_, trying next client" << std::endl;
        // Client was removed but queue wasn't cleaned up properly
        return try_unblock_clients_for_key(key, kv_store); // Try next client
    }

    std::cout << "[UNBLOCK LOG] Client " << client_fd << " found in client_info_" << std::endl;

    // Pop element from list
    std::string element = std::move(lval->values.front());
    lval->values.erase(lval->values.begin());

    std::cout << "[UNBLOCK LOG] Popped element: '" << element << "' for client " << client_fd << std::endl;

    // If list is empty, remove it from store
    if (lval->values.empty())
    {
        std::cout << "[UNBLOCK LOG] List is now empty, removing from store" << std::endl;
        kv_store.erase(store_it);
    }

    // Send response: array with [key, element]
    std::string response = "*2\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n" +
                           "$" + std::to_string(element.size()) + "\r\n" + element + "\r\n";

    std::cout << "[UNBLOCK LOG] SENDING UNBLOCK RESPONSE to client " << client_fd << ":" << std::endl;
    std::cout << "[UNBLOCK LOG] Response bytes: " << response << std::endl;

    ssize_t sent = send(client_fd, response.c_str(), response.size(), 0);
    if (sent < 0)
    {
        std::cerr << "[UNBLOCK LOG] ERROR: Failed to send response to unblocked client " << client_fd << std::endl;
    }
    else
    {
        std::cout << "[UNBLOCK LOG] SUCCESS: Sent " << sent << " bytes to client " << client_fd << " (expected " << response.size() << ")" << std::endl;
        std::cout << "Unblocked client " << client_fd << " with element from key: " << key << std::endl;
    }

    // Remove client from all queues (they're no longer blocked)
    std::cout << "[UNBLOCK LOG] Removing client " << client_fd << " from all blocked queues" << std::endl;
    remove_blocked_client(client_fd);

    return true;
}

void BlockingManager::check_timeouts()
{
    std::cout << "[TIMEOUT LOG] check_timeouts() called" << std::endl;
    auto now = std::chrono::steady_clock::now();
    std::vector<int> timed_out_clients;

    for (const auto &[client_fd, blocked_client_ptr] : client_info_)
    {
        // Skip indefinite blocking clients
        if (blocked_client_ptr->is_indefinite)
        {
            std::cout << "[TIMEOUT LOG] Client " << client_fd << " has indefinite blocking, skipping" << std::endl;
            continue;
        }

        // Calculate timeout time: block_start + timeout duration
        auto timeout_time = blocked_client_ptr->block_start + blocked_client_ptr->timeout;

        if (now >= timeout_time)
        {
            std::cout << "[TIMEOUT LOG] Client " << client_fd << " has timed out" << std::endl;
            timed_out_clients.push_back(client_fd);
        }
        else
        {
            auto remaining = timeout_time - now;
            std::cout << "[TIMEOUT LOG] Client " << client_fd << " has " << remaining.count() << " seconds remaining" << std::endl;
        }
    }

    if (timed_out_clients.empty())
    {
        std::cout << "[TIMEOUT LOG] No clients have timed out" << std::endl;
        return;
    }

    // Send NIL responses and remove timed-out clients
    for (int client_fd : timed_out_clients)
    {
        std::cout << "[TIMEOUT LOG] SENDING NIL RESPONSE to timed-out client " << client_fd << std::endl;
        const char *nil_response = "*-1\r\n"; // NIL array response
        ssize_t sent = send(client_fd, nil_response, strlen(nil_response), 0);

        std::cout << "[TIMEOUT LOG] Client " << client_fd << " sent NIL (" << sent << " bytes)" << std::endl;
        std::cout << "Client " << client_fd << " timed out on BLPOP" << std::endl;
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