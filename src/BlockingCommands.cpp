#include "BlockingCommands.hpp"
#include "BlockingManager.hpp"
#include "Commands.hpp" // For Store, ListValue, RESP constants
#include <sys/socket.h>
#include <cstring>
#include <iostream>
#include <string>

void handle_blpop(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    // BLPOP key timeout
    if (parts.size() != 3)
    {
        const char *error_msg = "-ERR wrong number of arguments for 'blpop' command\r\n";
        send(client_fd, error_msg, strlen(error_msg), 0);
        return;
    }

    const std::string key(parts[1]);

    // Parse timeout (last argument)
    int timeout_seconds;
    try
    {
        timeout_seconds = std::stoi(std::string(parts[2]));
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

    // Try to pop from the list immediately
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
            std::string response = "*2\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n" +
                                   "$" + std::to_string(element.size()) + "\r\n" + element + "\r\n";

            send(client_fd, response.c_str(), response.size(), 0);

            std::cout << "BLPOP immediate return for client " << client_fd
                      << " from key: " << key << std::endl;
            return;
        }
    }

    // No elements available - block the client
    std::vector<std::string> keys = {key};

    if (timeout_seconds == 0)
    {
        // Block indefinitely - use a very large timeout (e.g., 10 years)
        auto infinite_timeout = std::chrono::seconds(315360000); // ~10 years
        g_blocking_manager.add_blocked_client(client_fd, keys, infinite_timeout);
        std::cout << "BLPOP blocking client " << client_fd << " indefinitely on key: " << key << std::endl;
    }
    else
    {
        auto timeout = std::chrono::seconds(timeout_seconds);
        g_blocking_manager.add_blocked_client(client_fd, keys, timeout);
        std::cout << "BLPOP blocking client " << client_fd << " for " << timeout_seconds << " seconds on key: " << key << std::endl;
    }
}