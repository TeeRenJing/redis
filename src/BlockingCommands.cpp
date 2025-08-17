#include "BlockingCommands.hpp"
#include "BlockingManager.hpp"
#include "Commands.hpp" // For Store, ListValue, RESP constants
#include <sys/socket.h>
#include <cstring>
#include <iostream>
#include <string>

void handle_blpop(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    // BLPOP key [key ...] timeout
    if (parts.size() < 3)
    {
        const char *error_msg = "-ERR wrong number of arguments for 'blpop' command\r\n";
        send(client_fd, error_msg, strlen(error_msg), 0);
        return;
    }

    // Parse timeout (last argument)
    int timeout_seconds;
    try
    {
        timeout_seconds = std::stoi(std::string(parts.back()));
        if (timeout_seconds < 0)
        {
            const char *error_msg = "-ERR timeout is negative\r\n";
            send(client_fd, error_msg, strlen(error_msg), 0);
            return;
        }
    }
    catch (const std::exception &)
    {
        const char *error_msg = "-ERR timeout is not an integer\r\n";
        send(client_fd, error_msg, strlen(error_msg), 0);
        return;
    }

    // Extract keys (all arguments except command and timeout)
    std::vector<std::string> keys;
    keys.reserve(parts.size() - 2);
    for (size_t i = 1; i < parts.size() - 1; ++i)
    {
        keys.emplace_back(parts[i]);
    }

    // Try to pop from any available list immediately (check in order)
    for (const auto &key : keys)
    {
        auto it = kv_store.find(key);
        if (it != kv_store.end())
        {
            auto *lval = dynamic_cast<ListValue *>(it->second.get());
            if (lval && !lval->values.empty())
            {
                // Element available - pop and return immediately
                std::string element = std::move(lval->values.front());
                lval->values.erase(lval->values.begin());

                // If list is empty, remove it from store
                if (lval->values.empty())
                {
                    kv_store.erase(it);
                }

                // Send response: array with [key, element]
                std::string response = "*2\r\n" +
                                       "$" + std::to_string(key.size()) + "\r\n" + key + "\r\n" +
                                       "$" + std::to_string(element.size()) + "\r\n" + element + "\r\n";

                send(client_fd, response.c_str(), response.size(), 0);

                std::cout << "BLPOP immediate return for client " << client_fd
                          << " from key: " << key << std::endl;
                return;
            }
        }
    }

    // No elements available - block the client
    auto timeout = std::chrono::seconds(timeout_seconds);
    g_blocking_manager.add_blocked_client(client_fd, keys, timeout);

    // Note: Client will receive response when:
    // 1. An element becomes available (handled in LPUSH/RPUSH via try_unblock_clients_for_key)
    // 2. Timeout occurs (handled by check_timeouts)
    // 3. Client disconnects (should be handled in cleanup_client_on_disconnect)
}

void check_blocked_client_timeouts()
{
    g_blocking_manager.check_timeouts();
}

void cleanup_client_on_disconnect(int client_fd)
{
    if (g_blocking_manager.is_client_blocked(client_fd))
    {
        std::cout << "Cleaning up disconnected blocked client " << client_fd << std::endl;
        g_blocking_manager.remove_blocked_client(client_fd);
    }
}

void print_blocking_stats()
{
    std::cout << "Blocking Stats:" << std::endl;
    std::cout << "  Blocked clients: " << g_blocking_manager.get_blocked_client_count() << std::endl;
    std::cout << "  Blocked keys: " << g_blocking_manager.get_blocked_keys_count() << std::endl;
}