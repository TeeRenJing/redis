#include "Commands.hpp"
#include <string>
#include <sys/socket.h>
#include <cstring>

void handle_ping(int client_fd)
{
    send(client_fd, RESP_PONG, strlen(RESP_PONG), 0);
}

void handle_echo(int client_fd, const std::vector<std::string_view> &parts)
{
    if (parts.size() > 1)
    {
        const auto &echo_arg = parts[1];
        std::string response = "$" + std::to_string(echo_arg.size()) + "\r\n" + std::string(echo_arg) + "\r\n";
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
    std::string key = std::string(parts[1]);
    std::string value = std::string(parts[2]);
    auto expiry = std::chrono::steady_clock::time_point::max();

    if (parts.size() >= 5 && parts[3] == PX_ARG)
    {
        try
        {
            long long px = std::stoll(std::string(parts[4]));
            expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(px);
        }
        catch (...)
        {
            // Ignore invalid PX value
        }
    }

    kv_store[key] = {value, expiry};
    send(client_fd, RESP_OK, strlen(RESP_OK), 0);
}

void handle_get(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 2)
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }
    std::string key = std::string(parts[1]);
    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }

    // Check expiry only if key exists
    if (it->second.expiry != std::chrono::steady_clock::time_point::max() &&
        std::chrono::steady_clock::now() > it->second.expiry)
    {
        kv_store.erase(it); // erase expired key
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }

    // Key exists and is not expired
    const std::string &val = it->second.value;
    std::string response = "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
    send(client_fd, response.c_str(), response.size(), 0);
}