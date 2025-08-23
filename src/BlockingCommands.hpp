#pragma once

#include <vector>
#include <string_view>
#include <functional>
#include "Store.hpp"

// Command constants
constexpr const char *CMD_BLPOP = "BLPOP";
// BLPOP command implementation
void handle_blpop(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);

// Utility functions for blocking client management

void cleanup_client_on_disconnect(int client_fd);

void check_blocked_client_timeouts(std::function<void(int, const std::string &)> send_callback);
// Statistics/debugging functions
void print_blocking_stats();