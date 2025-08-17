#pragma once
#include <string>
#include <chrono>
#include <unordered_map>
#include <vector>
#include <memory>

struct ValueBase
{
    virtual ~ValueBase() = default;
    virtual bool is_expired() const { return false; }
};

struct StringValue : public ValueBase
{
    std::string value;
    std::chrono::steady_clock::time_point expiry;

    StringValue(const std::string &v, std::chrono::steady_clock::time_point e)
        : value(v), expiry(e) {}

    bool is_expired() const override
    {
        return expiry != std::chrono::steady_clock::time_point::max() &&
               std::chrono::steady_clock::now() > expiry;
    }
};

struct ListValue : public ValueBase
{
    std::vector<std::string> values;
};

using Store = std::unordered_map<std::string, std::unique_ptr<ValueBase>>;