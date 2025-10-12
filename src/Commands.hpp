#pragma once
#include "Store.hpp"
#include <string_view>
#include <vector>

constexpr const char *PX_ARG = "PX";

// RESP Protocol Constants
constexpr const char *RESP_OK = "+OK\r\n";
constexpr const char *RESP_PONG = "+PONG\r\n";
constexpr const char *RESP_NIL = "$-1\r\n";
constexpr const char *RESP_EMPTY_ARRAY = "*0\r\n";

// Error Responses
constexpr const char *RESP_ERR_GENERIC = "-ERR unknown command\r\n";
constexpr const char *RESP_ERR_XADD_EQ = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
constexpr const char *RESP_ERR_XADD_ZERO = "-ERR The ID specified in XADD must be greater than 0-0\r\n";

// Command Names
constexpr const char *CMD_PING = "PING";
constexpr const char *CMD_ECHO = "ECHO";
constexpr const char *CMD_SET = "SET";
constexpr const char *CMD_GET = "GET";
constexpr const char *CMD_LPUSH = "LPUSH";
constexpr const char *CMD_RPUSH = "RPUSH";
constexpr const char *CMD_LRANGE = "LRANGE";
constexpr const char *CMD_LLEN = "LLEN";
constexpr const char *CMD_LPOP = "LPOP";
constexpr const char *CMD_TYPE = "TYPE";
constexpr const char *CMD_XADD = "XADD";
constexpr const char *CMD_XRANGE = "XRANGE";

// Command Handlers
void handle_ping(int client_fd);
void handle_echo(int client_fd, const std::vector<std::string_view> &parts);
void handle_set(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_get(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_lpush(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_rpush(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_lrange(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_llen(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_lpop(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_type(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_xadd(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
void handle_xrange(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);
