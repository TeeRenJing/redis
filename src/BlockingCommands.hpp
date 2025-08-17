#pragma once

#include <vector>
#include <string_view>

// Forward declarations
class Store;

// BLPOP command implementation
void handle_blpop(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store);

// Utility functions for blocking client management
void check_blocked_client_timeouts();
void cleanup_client_on_disconnect(int client_fd);

// Statistics/debugging functions
void print_blocking_stats();