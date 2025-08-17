#include "Commands.hpp"
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
        std::string response;
        response.reserve(echo_arg.size() + 10);
        response += "$";
        response += std::to_string(echo_arg.size());
        response += "\r\n";
        response.append(echo_arg.data(), echo_arg.size());
        response += "\r\n";
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
            const auto px = std::stoll(std::string(parts[4]));
            expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(px);
        }
        catch (...)
        {
        }
    }

    kv_store[key] = std::make_unique<StringValue>(value, expiry);
    send(client_fd, RESP_OK, strlen(RESP_OK), 0);
}

void handle_get(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 2)
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }
    const auto key = std::string(parts[1]);
    const auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }

    if (const auto *sval = dynamic_cast<const StringValue *>(it->second.get()))
    {
        if (sval->is_expired())
        {
            kv_store.erase(it);
            send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
            return;
        }
        std::string response;
        response.reserve(sval->value.size() + 10);
        response += "$";
        response += std::to_string(sval->value.size());
        response += "\r\n";
        response += sval->value;
        response += "\r\n";
        send(client_fd, response.c_str(), response.size(), 0);
    }
    else
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
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
    const auto value = std::string(parts[2]);

    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        auto list = std::make_unique<ListValue>();
        list->values.emplace_back(value);
        kv_store.emplace(key, std::move(list));
        send(client_fd, ":1\r\n", 4, 0);
    }
    else if (auto *lval = dynamic_cast<ListValue *>(it->second.get()))
    {
        lval->values.emplace_back(value);
        std::string response = ":" + std::to_string(lval->values.size()) + "\r\n";
        send(client_fd, response.c_str(), response.size(), 0);
    }
    else
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
    }
}