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

void handle_rpush(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 3)
    {
        send(client_fd, RESP_NIL, strlen(RESP_NIL), 0);
        return;
    }
    const auto key = std::string(parts[1]);

    // Collect all values to push
    std::vector<std::string> values;
    values.reserve(parts.size() - 2);
    for (size_t i = 2; i < parts.size(); ++i)
        values.emplace_back(parts[i]);

    if (auto it = kv_store.find(key); it != kv_store.end())
    {
        if (auto *lval = dynamic_cast<ListValue *>(it->second.get()))
        {
            lval->values.insert(lval->values.end(), std::make_move_iterator(values.begin()), std::make_move_iterator(values.end()));
            const std::string response = ":" + std::to_string(lval->values.size()) + "\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
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
        kv_store.emplace(key, std::move(list));
        const std::string response = ":" + std::to_string(list->values.size()) + "\r\n";
        send(client_fd, response.c_str(), response.size(), 0);
    }
}