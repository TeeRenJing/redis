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
    std::cout << "[ADD_BLOCKED LOG] === ENTERING add_blocked_client ===" << std::endl;
    std::cout << "[ADD_BLOCKED LOG] client_fd: " << client_fd << std::endl;
    std::cout << "[ADD_BLOCKED LOG] keys.size(): " << keys.size() << std::endl;

    for (size_t i = 0; i < keys.size(); ++i)
    {
        std::cout << "[ADD_BLOCKED LOG] keys[" << i << "]: " << keys[i] << std::endl;
    }

    double timeout_value = timeout.count();
    std::cout << "[ADD_BLOCKED LOG] timeout.count(): " << timeout_value << std::endl;

    // Defensive check: if timeout is <= 0, treat as indefinite
    if (timeout_value <= 0.0)
    {
        std::cout << "[ADD_BLOCKED LOG] WARNING: timeout is <= 0 (" << timeout_value
                  << "), delegating to add_indefinitely_blocked_client()" << std::endl;
        add_indefinitely_blocked_client(client_fd, keys);
        std::cout << "[ADD_BLOCKED LOG] === EARLY EXITING add_blocked_client ===" << std::endl;
        return;
    }

    std::cout << "[ADD_BLOCKED LOG] Adding client " << client_fd
              << " with timeout " << timeout_value << " seconds" << std::endl;

    bool indefinite = (timeout_value >= 315360000); // 10 years or more considered indefinite

    // Create BlockedClient info
    auto blocked_client = std::make_unique<BlockedClient>(client_fd, keys, timeout);
    blocked_client->is_indefinite = indefinite;

    // Store client info
    client_info_[client_fd] = std::move(blocked_client);

    // Add client to each key's waiting queue
    for (const auto &key : keys)
    {
        blocked_clients_[key].push(client_fd);
    }

    std::cout << "[ADD_BLOCKED LOG] Client " << client_fd
              << " successfully added to blocking manager" << std::endl;
    std::cout << "[ADD_BLOCKED LOG] === EXITING add_blocked_client ===" << std::endl;
}

void BlockingManager::add_indefinitely_blocked_client(int client_fd, const std::vector<std::string> &keys)
{
    std::cout << "[ADD_INDEFINITE_BLOCKED LOG] === ENTERING add_indefinitely_blocked_client ===" << std::endl;
    std::cout << "[ADD_INDEFINITE_BLOCKED LOG] client_fd: " << client_fd << std::endl;
    std::cout << "[ADD_INDEFINITE_BLOCKED LOG] keys.size(): " << keys.size() << std::endl;

    for (size_t i = 0; i < keys.size(); ++i)
    {
        std::cout << "[ADD_INDEFINITE_BLOCKED LOG] keys[" << i << "]: " << keys[i] << std::endl;
    }

    // Use a timeout value >= 315360000 to indicate indefinite blocking
    constexpr double INDEFINITE_TIMEOUT_SECONDS = 315360000.0; // 10 years
    std::chrono::duration<double> indefinite_timeout(INDEFINITE_TIMEOUT_SECONDS);

    std::cout << "[ADD_INDEFINITE_BLOCKED LOG] Calling add_blocked_client with indefinite timeout ("
              << INDEFINITE_TIMEOUT_SECONDS << " seconds)" << std::endl;

    // Delegate to the existing function
    add_blocked_client(client_fd, keys, indefinite_timeout);

    std::cout << "[ADD_INDEFINITE_BLOCKED LOG] === EXITING add_indefinitely_blocked_client ===" << std::endl;
}

void BlockingManager::remove_blocked_client(int client_fd)
{
    auto it = client_info_.find(client_fd);
    if (it == client_info_.end())
        return;

    std::cout << "Removing blocked client " << client_fd << std::endl;

    // Remove from all key queues
    // This is still O(n*m) but acceptable for single-threaded since no contention
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

bool BlockingManager::try_unblock_clients_for_key(const std::string &key, Store &kv_store, std::function<void(int, const std::string &)> send_callback)
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

    auto *list_value = dynamic_cast<ListValue *>(store_it->second.get());
    if (!list_value || list_value->values.empty())
    {
        std::cout << "[UNBLOCK LOG] Key exists but list is empty or not a list: " << key << std::endl;
        return false;
    }

    std::cout << "[UNBLOCK LOG] List has " << list_value->values.size() << " elements" << std::endl;

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
        return try_unblock_clients_for_key(key, kv_store, send_callback); // Try next client
    }

    std::cout << "[UNBLOCK LOG] Client " << client_fd << " found in client_info_" << std::endl;

    // Pop element from list
    std::string element = std::move(list_value->values.front());
    list_value->values.erase(list_value->values.begin());

    std::cout << "[UNBLOCK LOG] Popped element: '" << element << "' for client " << client_fd << std::endl;

    // If list is empty, remove it from store
    if (list_value->values.empty())
    {
        std::cout << "[UNBLOCK LOG] List is now empty, removing from store" << std::endl;
        kv_store.erase(store_it);
    }

    // Send response: array with [key, element]
    std::string response = "*2\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n" +
                           "$" + std::to_string(element.size()) + "\r\n" + element + "\r\n";

    std::cout << "[UNBLOCK LOG] SENDING UNBLOCK RESPONSE to client " << client_fd << ":" << std::endl;
    std::cout << "[UNBLOCK LOG] Response bytes: " << response << std::endl;

    // Use callback instead of direct send() to handle non-blocking sockets
    send_callback(client_fd, response);

    std::cout << "[UNBLOCK LOG] Response queued for client " << client_fd << std::endl;
    std::cout << "Unblocked client " << client_fd << " with element from key: " << key << std::endl;

    // Remove client from all queues (they're no longer blocked)
    std::cout << "[UNBLOCK LOG] Removing client " << client_fd << " from all blocked queues" << std::endl;
    remove_blocked_client(client_fd);

    return true;
}
void BlockingManager::check_timeouts(std::function<void(int, const std::string &)> send_callback)
{
    std::cout << "[TIMEOUT LOG] check_timeouts() called" << std::endl;

    // Capture current time using steady_clock for measuring durations
    // (steady_clock is monotonic, not affected by system clock changes)
    auto now = std::chrono::steady_clock::now();

    // Collect clients that have exceeded their blocking timeout
    // Using a separate vector ensures we don't modify client_info_ while iterating
    std::vector<int> timed_out_clients;

    // Iterating over an unordered_map using range-based for loop
    // Key: client_fd, Value: unique_ptr<BlockedClient>
    for (const auto &[client_fd, blocked_client_ptr] : client_info_)
    {
        // Skip clients that are intentionally blocked indefinitely
        if (blocked_client_ptr->is_indefinite)
        {
            std::cout << "[TIMEOUT LOG] Client " << client_fd
                      << " has indefinite blocking, skipping" << std::endl;
            continue;
        }

        // Calculate absolute timeout point:
        //   block_start (when client started waiting) + timeout (duration requested)
        auto timeout_time = blocked_client_ptr->block_start + blocked_client_ptr->timeout;

        // Compare against current time
        if (now >= timeout_time)
        {
            // Client has exceeded its timeout
            std::cout << "[TIMEOUT LOG] Client " << client_fd << " has timed out" << std::endl;
            timed_out_clients.push_back(client_fd);
        }
        else
        {
            // Calculate remaining time in SECONDS (human-friendly)
            auto remaining_sec = std::chrono::duration_cast<std::chrono::seconds>(timeout_time - now);

            // Good practice: always log in consistent units
            std::cout << "[TIMEOUT LOG] Client " << client_fd
                      << " has " << remaining_sec.count() << " seconds remaining" << std::endl;
        }
    }

    // Early exit if no clients timed out
    if (timed_out_clients.empty())
    {
        std::cout << "[TIMEOUT LOG] No clients have timed out" << std::endl;
        return;
    }

    // Send NIL responses to timed-out clients and remove them from blocking manager
    for (int client_fd : timed_out_clients)
    {
        std::cout << "[TIMEOUT LOG] SENDING NIL RESPONSE to timed-out client "
                  << client_fd << std::endl;

        // RESP protocol: NIL response for arrays is "*-1\r\n"
        const std::string nil_response = "*-1\r\n";

        // Using callback pattern instead of direct send() to respect non-blocking I/O
        send_callback(client_fd, nil_response);

        std::cout << "[TIMEOUT LOG] Client " << client_fd
                  << " queued NIL response and will be removed" << std::endl;

        // Remove client from all blocking structures (RAII cleans up unique_ptr automatically)
        remove_blocked_client(client_fd);
    }
}

// New method to check if a key can immediately satisfy a blocking operation
bool BlockingManager::can_immediate_pop(const std::string &key, Store &kv_store) const
{
    auto store_iter = kv_store.find(key);
    if (store_iter == kv_store.end())
        return false;

    auto *list_value = dynamic_cast<ListValue *>(store_iter->second.get());
    return list_value && !list_value->values.empty();
}

// New method to get all keys that a client is waiting for
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