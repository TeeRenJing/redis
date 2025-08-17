#pragma once
#include <string>
#include <chrono>
#include <unordered_map>
#include <variant>
#include <vector>

struct ValueWithExpiry {
    std::string value;
    std::chrono::steady_clock::time_point expiry;
};

using ListType = std::vector<std::string>;
using StoreValue = std::variant<ValueWithExpiry, ListType>;
using Store = std::unordered_map<std::string, StoreValue>;