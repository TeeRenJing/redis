#include "BlockingCommands.hpp"
#include "BlockingManager.hpp"
#include "Commands.hpp" // For Store, ListValue, RESP constants
#include <sys/socket.h>
#include <cstring>
#include <iostream>
#include <string>

void handle_blpop(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    std::cout << "[BLPOP LOG] Client " << client_fd << " called BLPOP" << std::endl;

    // BLPOP key timeout
    if (parts.size() != 3)
    {
        std::cout << "[BLPOP LOG] Client " << client_fd << " - ERROR: wrong number of arguments (" << parts.size() << ")" << std::endl;
        const char *error_msg = "-ERR wrong number of arguments for 'blpop' command\r\n";
        send(client_fd, error_msg, strlen(error_msg), 0);
        std::cout << "[BLPOP LOG] Client " << client_fd << " - SENT ERROR RESPONSE" << std::endl;
        return;
    }

    const std::string key(parts[1]);
    std::cout << "[BLPOP LOG] Client " << client_fd << " - Key: " << key << std::endl;

    // Parse timeout (last argument)
    double timeout_seconds;
    try
    {
        timeout_seconds = std::stod(std::string(parts[2]));
        std::cout << "[BLPOP LOG] Client " << client_fd << " - Timeout: " << timeout_seconds << std::endl;

        if (timeout_seconds < 0.0)
        {
            std::cout << "[BLPOP LOG] Client " << client_fd << " - ERROR: timeout is negative" << std::endl;
            const char *error_msg = "-ERR timeout is negative\r\n";
            send(client_fd, error_msg, strlen(error_msg), 0);
            std::cout << "[BLPOP LOG] Client " << client_fd << " - SENT NEGATIVE TIMEOUT ERROR" << std::endl;
            return;
        }
    }
    catch (const std::exception &)
    {
        std::cout << "[BLPOP LOG] Client " << client_fd << " - ERROR: timeout is not a number" << std::endl;
        const char *error_msg = "-ERR timeout is not a number\r\n";
        send(client_fd, error_msg, strlen(error_msg), 0);
        std::cout << "[BLPOP LOG] Client " << client_fd << " - SENT INVALID TIMEOUT ERROR" << std::endl;
        return;
    }

    // Try to pop from the list immediately
    std::cout << "[BLPOP LOG] Client " << client_fd << " - Checking if key exists immediately..." << std::endl;
    auto it = kv_store.find(key);
    if (it != kv_store.end())
    {
        std::cout << "[BLPOP LOG] Client " << client_fd << " - Key found in store" << std::endl;
        auto *lval = dynamic_cast<ListValue *>(it->second.get());
        if (lval)
        {
            std::cout << "[BLPOP LOG] Client " << client_fd << " - Key is a list with " << lval->values.size() << " elements" << std::endl;
            if (!lval->values.empty())
            {
                std::cout << "[BLPOP LOG] Client " << client_fd << " - IMMEDIATE POP: Element available!" << std::endl;

                // Element available - pop and return immediately
                std::string element = std::move(lval->values.front());
                lval->values.erase(lval->values.begin());

                std::cout << "[BLPOP LOG] Client " << client_fd << " - Popped element: '" << element << "'" << std::endl;

                // If list is empty, remove it from store
                if (lval->values.empty())
                {
                    std::cout << "[BLPOP LOG] Client " << client_fd << " - List is now empty, removing from store" << std::endl;
                    kv_store.erase(it);
                }

                // Send response: array with [key, element]
                std::string response = "*2\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n" +
                                       "$" + std::to_string(element.size()) + "\r\n" + element + "\r\n";

                std::cout << "[BLPOP LOG] Client " << client_fd << " - SENDING IMMEDIATE SUCCESS RESPONSE: " << std::endl;
                std::cout << "[BLPOP LOG] Response bytes: " << response << std::endl;

                ssize_t sent = send(client_fd, response.c_str(), response.size(), 0);
                std::cout << "[BLPOP LOG] Client " << client_fd << " - SENT " << sent << " bytes (expected " << response.size() << ")" << std::endl;

                std::cout << "BLPOP immediate return for client " << client_fd
                          << " from key: " << key << std::endl;
                return;
            }
            else
            {
                std::cout << "[BLPOP LOG] Client " << client_fd << " - List exists but is empty" << std::endl;
            }
        }
        else
        {
            std::cout << "[BLPOP LOG] Client " << client_fd << " - Key exists but is not a list" << std::endl;
        }
    }
    else
    {
        std::cout << "[BLPOP LOG] Client " << client_fd << " - Key does not exist in store" << std::endl;
    }

    // No elements available - block the client
    std::cout << "[BLPOP LOG] Client " << client_fd << " - No elements available, will block..." << std::endl;
    std::vector<std::string> keys = {key};

    // Block client accordingly - FIX THE TIMEOUT HANDLING HERE
    std::cout << "[BLPOP LOG] Client " << client_fd << " - TIMEOUT DEBUG: timeout_seconds=" << timeout_seconds << std::endl;
    std::cout << "[BLPOP LOG] Client " << client_fd << " - TIMEOUT DEBUG: timeout_seconds == 0.0? " << (timeout_seconds == 0.0 ? "YES" : "NO") << std::endl;

    if (timeout_seconds == 0.0)
    {
        std::cout << "[BLPOP LOG] Client " << client_fd << " - INFINITE TIMEOUT, CALLING add_indefinitely_blocked_client..." << std::endl;
        g_blocking_manager.add_indefinitely_blocked_client(client_fd, keys);
        std::cout << "[BLPOP LOG] Client " << client_fd << " - RETURNED from add_indefinitely_blocked_client" << std::endl;
    }
    else
    {
        std::chrono::duration<double> timeout(timeout_seconds);
        std::cout << "[BLPOP LOG] Client " << client_fd << " - FINITE TIMEOUT CREATED: " << timeout.count() << " seconds" << std::endl;
        std::cout << "[BLPOP LOG] Client " << client_fd << " - CALLING add_blocked_client with finite timeout..." << std::endl;
        g_blocking_manager.add_blocked_client(client_fd, keys, timeout);
        std::cout << "[BLPOP LOG] Client " << client_fd << " - RETURNED from add_blocked_client" << std::endl;
    }

    std::cout << "[BLPOP LOG] Client " << client_fd << " - handle_blpop FINISHED (client is now blocked)" << std::endl;
}

// In BlockingCommands.cpp
void check_blocked_client_timeouts(std::function<void(int, const std::string &)> send_callback)
{
    g_blocking_manager.check_timeouts(send_callback);
}

void cleanup_client_on_disconnect(int client_fd)
{
    std::cout << "[CLEANUP LOG] Client " << client_fd << " disconnected" << std::endl;
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