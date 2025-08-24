#include "Commands.hpp"
#include "BlockingManager.hpp" // Include for blocking support
#include <string>
#include <sys/socket.h>
#include <cstring>
#include <iostream>

void handle_ping(int client_fd)
{
    send(client_fd, RESP_PONG, strlen(RESP_PONG), 0);
}

void handle_echo(int client_fd, const std::vector<std::string_view> &parts)
{
    if (parts.size() > 1)
    {
        const auto &echo_arg = parts[1];
        std::string response = "$" + std::to_string(echo_arg.size()) + "\r\n" +
                               std::string(echo_arg) + "\r\n";
        send(client_fd, response.c_str(), response.size(), 0);
    }
    else
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
    }
}

void handle_set(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 3)
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }
    const auto key = std::string(parts[1]);
    const auto value = std::string(parts[2]);
    auto expiry = std::chrono::steady_clock::time_point::max();

    if (parts.size() >= 5 && parts[3] == PX_ARG)
    {
        try
        {
            auto px = std::stoll(std::string(parts[4]));
            expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(px);
        }
        catch (...)
        {
        }
    }

    kv_store.insert_or_assign(key, std::make_unique<StringValue>(value, expiry));
    send(client_fd, RESP_OK, strlen(RESP_OK), 0);
}

void handle_get(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 2)
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }
    auto key = std::string(parts[1]);
    if (auto it = kv_store.find(key); it != kv_store.end())
    {
        if (auto *sval = dynamic_cast<const StringValue *>(it->second.get()))
        {
            if (sval->is_expired())
            {
                kv_store.erase(it);
                send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
                return;
            }
            std::string response = "$" + std::to_string(sval->value.size()) + "\r\n" +
                                   sval->value + "\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
        }
        else
        {
            send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        }
    }
    else
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
    }
}

void handle_lpush(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 3)
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }
    const auto key = std::string(parts[1]);
    bool operation_successful = false;

    if (auto it = kv_store.find(key); it != kv_store.end())
    {
        if (auto *lval = dynamic_cast<ListValue *>(it->second.get()))
        {
            // Process elements from LEFT TO RIGHT in the command
            // Each element gets pushed to the LEFT (front) of the list
            for (size_t i = 2; i < parts.size(); ++i)
            {
                lval->values.emplace(lval->values.begin(), parts[i]);
            }

            const std::string response = ":" + std::to_string(lval->values.size()) + "\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
            operation_successful = true;
        }
        else
        {
            send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        }
    }
    else
    {
        // For new lists, simulate the same process
        auto list = std::make_unique<ListValue>();

        // Process arguments left to right, each going to front of list
        for (size_t i = 2; i < parts.size(); ++i)
        {
            list->values.emplace(list->values.begin(), parts[i]);
        }

        const std::string response = ":" + std::to_string(list->values.size()) + "\r\n";
        kv_store.emplace(key, std::move(list));
        send(client_fd, response.c_str(), response.size(), 0);
        operation_successful = true;
    }

    // Try to unblock waiting clients if operation was successful
    // In handle_lpush and handle_rpush
    if (operation_successful)
    {
        auto send_callback = [](int client_fd, const std::string &response)
        {
            send(client_fd, response.c_str(), response.size(), 0);
        };
        g_blocking_manager.try_unblock_clients_for_key(key, kv_store, send_callback);
    }
}

void handle_rpush(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 3)
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }
    const auto key = std::string(parts[1]);
    bool operation_successful = false;

    // Collect all values to push
    std::vector<std::string> values;
    values.reserve(parts.size() - 2);
    for (size_t i = 2; i < parts.size(); ++i)
        values.emplace_back(parts[i]);

    if (auto it = kv_store.find(key); it != kv_store.end())
    {
        if (auto *lval = dynamic_cast<ListValue *>(it->second.get()))
        {
            lval->values.insert(lval->values.end(),
                                std::make_move_iterator(values.begin()),
                                std::make_move_iterator(values.end()));
            const std::string response = ":" + std::to_string(lval->values.size()) + "\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
            operation_successful = true;
        }
        else
        {
            send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        }
    }
    else
    {
        auto list = std::make_unique<ListValue>();
        list->values = std::move(values);
        const std::string response = ":" + std::to_string(list->values.size()) + "\r\n";
        kv_store.emplace(key, std::move(list));
        send(client_fd, response.c_str(), response.size(), 0);
        operation_successful = true;
    }

    // Try to unblock waiting clients if operation was successful
    // In handle_lpush and handle_rpush
    if (operation_successful)
    {
        auto send_callback = [](int client_fd, const std::string &response)
        {
            send(client_fd, response.c_str(), response.size(), 0);
        };
        g_blocking_manager.try_unblock_clients_for_key(key, kv_store, send_callback);
    }
}

void handle_lrange(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 4)
    {
        send(client_fd, RESP_EMPTY_ARRAY, strlen(RESP_EMPTY_ARRAY), 0);
        return;
    }
    const auto key = std::string(parts[1]);
    int start = 0, stop = 0;
    try
    {
        start = std::stoi(std::string(parts[2]));
        stop = std::stoi(std::string(parts[3]));
    }
    catch (...)
    {
        send(client_fd, RESP_EMPTY_ARRAY, strlen(RESP_EMPTY_ARRAY), 0);
        return;
    }

    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        send(client_fd, RESP_EMPTY_ARRAY, strlen(RESP_EMPTY_ARRAY), 0);
        return;
    }
    auto *lval = dynamic_cast<ListValue *>(it->second.get());
    if (!lval)
    {
        send(client_fd, RESP_EMPTY_ARRAY, strlen(RESP_EMPTY_ARRAY), 0);
        return;
    }

    const auto &values = lval->values;
    int len = static_cast<int>(values.size());
    if (start < 0)
        start = len + start;
    if (stop < 0)
        stop = len + stop;
    if (start < 0)
        start = 0;
    if (stop < 0)
        stop = 0;
    if (start >= len || start > stop)
    {
        send(client_fd, RESP_EMPTY_ARRAY, strlen(RESP_EMPTY_ARRAY), 0);
        return;
    }
    if (stop >= len)
        stop = len - 1;

    int count = stop - start + 1;
    if (count <= 0)
    {
        send(client_fd, RESP_EMPTY_ARRAY, strlen(RESP_EMPTY_ARRAY), 0);
        return;
    }

    std::string response = "*" + std::to_string(count) + "\r\n";
    for (int i = start; i <= stop; ++i)
    {
        response += "$" + std::to_string(values[i].size()) + "\r\n" + values[i] + "\r\n";
    }
    send(client_fd, response.c_str(), response.size(), 0);
}

void handle_llen(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 2)
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }
    const auto key = std::string(parts[1]);
    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        // the response of LLEN command for a non-existent list. is 0, which is RESP Encoded as :0\r\n
        std::string response = ":0\r\n";
        send(client_fd, response.c_str(), response.size(), 0);
        return;
    }
    auto *lval = dynamic_cast<ListValue *>(it->second.get());
    if (!lval)
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }
    std::string response = ":" + std::to_string(lval->values.size()) + "\r\n";
    send(client_fd, response.c_str(), response.size(), 0);
}

void handle_lpop(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    // LPOP key [count]
    // Returns:
    // - Single element if no count specified
    // - Array of elements if count specified
    // - NIL if list doesn't exist or is empty

    if (parts.size() < 2)
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }

    const auto key = std::string(parts[1]);
    int count = 1; // Default count is 1

    // Parse optional count parameter
    if (parts.size() > 2)
    {
        try
        {
            count = std::stoi(std::string(parts[2]));
            if (count < 0)
            {
                // Redis treats negative count as error
                send(client_fd, "-ERR value is not an integer or out of range\r\n", 52, 0);
                return;
            }
        }
        catch (const std::exception &)
        {
            send(client_fd, "-ERR value is not an integer or out of range\r\n", 52, 0);
            return;
        }
    }

    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        // Key doesn't exist
        if (parts.size() > 2)
        {
            // Return empty array for LPOP key count
            send(client_fd, RESP_EMPTY_ARRAY, strlen(RESP_EMPTY_ARRAY), 0);
        }
        else
        {
            // Return NIL for LPOP key
            send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        }
        return;
    }

    auto *lval = dynamic_cast<ListValue *>(it->second.get());
    if (!lval)
    {
        // Key exists but is not a list
        send(client_fd, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", 69, 0);
        return;
    }

    if (lval->values.empty())
    {
        // List is empty
        if (parts.size() > 2)
        {
            // Return empty array for LPOP key count
            send(client_fd, RESP_EMPTY_ARRAY, strlen(RESP_EMPTY_ARRAY), 0);
        }
        else
        {
            // Return NIL for LPOP key
            send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        }
        return;
    }

    const size_t list_size = lval->values.size();
    const size_t elements_to_pop = std::min(static_cast<size_t>(count), list_size);

    std::string response;

    if (parts.size() <= 2)
    {
        // LPOP key - return single element as bulk string
        const std::string &element = lval->values.front();
        response = "$" + std::to_string(element.size()) + "\r\n" + element + "\r\n";

        // Remove the element
        lval->values.erase(lval->values.begin());
    }
    else
    {
        // LPOP key count - return array of elements
        response = "*" + std::to_string(elements_to_pop) + "\r\n";

        // Add each element to response
        for (size_t i = 0; i < elements_to_pop; ++i)
        {
            const std::string &element = lval->values[i];
            response += "$" + std::to_string(element.size()) + "\r\n" + element + "\r\n";
        }

        // Remove the elements (more efficient to erase range)
        lval->values.erase(lval->values.begin(), lval->values.begin() + elements_to_pop);
    }

    // If list is now empty, remove it from the store
    if (lval->values.empty())
    {
        kv_store.erase(it);
    }
    // Send the response
    send(client_fd, response.c_str(), response.size(), 0);
}

// TYPE command: returns the type of value stored at a given key
// Possible return values: string, list, set, zset, hash, stream, or "none" if the key doesn't exist.
void handle_type(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    // Defensive programming: always validate input length
    if (parts.size() < 2)
    {
        // RESP simple string for non-existent key type: "none"
        const char *response = "+none\r\n";
        send(client_fd, response, strlen(response), 0);
        return;
    }

    const auto key = std::string(parts[1]);
    auto it = kv_store.find(key);

    if (it == kv_store.end())
    {
        // Key does not exist
        const char *response = "+none\r\n";
        send(client_fd, response, strlen(response), 0);
        return;
    }

    // We use dynamic_cast here to safely check the runtime type
    // Principle: Prefer dynamic_cast only when type information matters (RTTI usage).
    std::string type_str;
    if (dynamic_cast<StringValue *>(it->second.get()))
    {
        type_str = "string";
    }
    else if (dynamic_cast<ListValue *>(it->second.get()))
    {
        type_str = "list";
    }
    else
    {
        // Extend this when adding new types like set, hash, etc.
        type_str = "none";
    }

    // RESP simple string reply: starts with '+'
    std::string response = "+" + type_str + "\r\n";
    send(client_fd, response.c_str(), response.size(), 0);
}
