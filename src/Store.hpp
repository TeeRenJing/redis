#pragma once
#include <string>
#include <chrono>
#include <unordered_map>

struct ValueWithExpiry {
    std::string value;
    std::chrono::steady_clock::time_point expiry;
};

using Store = std::unordered_map<std::string, ValueWithExpiry>;