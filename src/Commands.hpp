#pragma once
#include "Store.hpp"
#include <string_view>
#include <vector>

constexpr const char *RESP_OK = "+OK\r\n";
constexpr const char *RESP_PONG = "+PONG\r\n";
constexpr const char *RESP_NIL = "$-1\r\n";
constexpr const char *RESP_EMPTY_ARRAY = "*0\r\n";
constexpr const char *PX_ARG = "px";
constexpr const char *CMD_PING = "PING";
constexpr const char *CMD_ECHO = "ECHO";
constexpr const char *CMD_SET = "SET";
constexpr const char *CMD_GET = "GET";
constexpr const char *CMD_LPUSH = "LPUSH";
constexpr const char *CMD_RPUSH = "RPUSH";
constexpr const char *CMD_LRANGE = "LRANGE";
// The LLEN command is used to query a list's length. It returns a RESP-encoded integer.
constexpr const char *CMD_LLEN = "LLEN";

void handle_ping(int client_fd);
void handle_echo(int client_fd, const std::vector<std::string_view> &parts);
void handle_set(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_get(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_lpush(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_rpush(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_lrange(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_llen(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
// Additional command handlers can be added here as needed.
// For example, you might want to implement a command to delete keys, or to check if a key exists.
// This modular approach allows for easy expansion of the command set without modifying existing code.
// Each command handler can be implemented in a separate source file if desired, keeping the code organized and maintainable.