#include "Commands.hpp"
#include "BlockingManager.hpp"
#include <sys/socket.h>
#include <unordered_map>
#include <string>
#include <string_view>
#include <vector>
#include <cstring> // for std::strlen (if ever needed)

// ============================
// RESP PROTOCOL CONSTANTS
// ============================
// Using constexpr std::string_view allows the compiler to know the data and size at compile-time.
// This removes the need for strlen() calls at runtime, improving performance.
constexpr std::string_view RESP_PONG = "+PONG\r\n";
constexpr std::string_view RESP_OK = "+OK\r\n";
constexpr std::string_view RESP_NIL = "$-1\r\n";
constexpr std::string_view RESP_EMPTY_ARRAY = "*0\r\n";
constexpr std::string_view RESP_ERR_GENERIC = "-ERR unknown command\r\n";
constexpr std::string_view RESP_ERR_XADD_EQ = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
constexpr std::string_view RESP_ERR_XADD_ZERO = "-ERR The ID specified in XADD must be greater than 0-0\r\n";

// ============================
// Example Global Store
// ============================
// These are kept as std::string → std::string because Redis keys/values are binary-safe.
// We use std::string_view only temporarily when parsing to avoid extra allocations.
static std::unordered_map<std::string, std::string> kv_store;

// ============================
// SEND HELPER
// ============================
// A small wrapper to reduce repetitive .data()/.size() boilerplate.
inline void send_response(int client_fd, std::string_view resp)
{
    // Using .data() and .size() is safer than strlen() because it works with binary data too.
    send(client_fd, resp.data(), resp.size(), 0);
}

// ============================
// PING COMMAND
// ============================
// Very frequent → optimized using compile-time constant.
void handle_ping(int client_fd)
{
    send_response(client_fd, RESP_PONG);
}

// ============================
// GET COMMAND
// ============================
// Demonstrates string_view → string only when necessary.
void handle_get(int client_fd, const std::vector<std::string_view> &parts)
{
    if (parts.size() != 2)
    {
        send_response(client_fd, RESP_ERR_GENERIC);
        return;
    }

    const std::string key(parts[1]); // convert only once here for map lookup
    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        send_response(client_fd, RESP_NIL); // RESP NIL for non-existing key
    }
    else
    {
        const std::string &val = it->second;
        // Build bulk string: "$<len>\r\n<value>\r\n"
        std::string resp = "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
        send(client_fd, resp.data(), resp.size(), 0);
    }
}

// ============================
// SET COMMAND
// ============================
void handle_set(int client_fd, const std::vector<std::string_view> &parts)
{
    if (parts.size() != 3)
    {
        send_response(client_fd, RESP_ERR_GENERIC);
        return;
    }

    // std::string_view → std::string conversion only once (map requires owning type)
    kv_store[std::string(parts[1])] = std::string(parts[2]);
    send_response(client_fd, RESP_OK);
}

// ============================
// XADD COMMAND
// ============================
// Validates stream ID rules (similar to Redis) and sends proper RESP simple errors.
// Handles the XADD command: appends an entry to a Redis-like stream with a specified ID.
// Arguments format: XADD <key> <id> field value [field value ...]
// Example: XADD mystream 1-0 foo bar
void handle_xadd(int client_fd, const std::vector<std::string_view> &parts, Store &store)
{
    // Ensure minimal argument count: XADD requires at least key, id, and one field-value pair
    if (parts.size() < 4)
    {
        send_response(client_fd, RESP_ERR_GENERIC); // Send a generic error (RESP simple error)
        return;
    }

    const std::string key(parts[1]);         // Extract the stream key
    const std::string_view id_sv = parts[2]; // Extract the ID (e.g., "1-1")

    // Parse the stream ID in the format milliseconds-seq
    auto dash = id_sv.find('-');
    if (dash == std::string_view::npos)
    {
        send_response(client_fd, RESP_ERR_GENERIC);
        return;
    }

    // Convert both parts to integers: this ensures lexicographic order = numeric order
    uint64_t ms = std::stoull(std::string(id_sv.substr(0, dash)));
    uint64_t seq = std::stoull(std::string(id_sv.substr(dash + 1)));

    // Rule 1: ID must be strictly greater than 0-0
    if (ms == 0 && seq == 0)
    {
        send_response(client_fd, RESP_ERR_XADD_ZERO); // "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        return;
    }

    // Look up the stream, create if missing
    auto it = store.find(key);
    StreamValue *stream;
    if (it == store.end())
    {
        auto new_stream = std::make_unique<StreamValue>();
        stream = new_stream.get();
        store[key] = std::move(new_stream);
    }
    else
    {
        stream = dynamic_cast<StreamValue *>(it->second.get());
        if (!stream)
        {
            send_response(client_fd, RESP_ERR_GENERIC); // Wrong type
            return;
        }
    }

    // Rule 2: New ID must be greater than the last entry
    if (!stream->entries.empty())
    {
        const auto &last_id = stream->entries.back().id;
        auto dash_last = last_id.find('-');
        uint64_t last_ms = std::stoull(last_id.substr(0, dash_last));
        uint64_t last_seq = std::stoull(last_id.substr(dash_last + 1));

        if (ms < last_ms || (ms == last_ms && seq <= last_seq))
        {
            send_response(client_fd, RESP_ERR_XADD_EQ); // "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
            return;
        }
    }

    // Construct and append the stream entry
    StreamEntry entry;
    entry.id = std::string(id_sv);
    entry.fields.reserve((parts.size() - 3) / 2); // Preallocate field-value pairs for efficiency

    // Populate field-value pairs (pairs start from parts[3])
    for (size_t i = 3; i + 1 < parts.size(); i += 2)
    {
        entry.fields.emplace(std::string(parts[i]), std::string(parts[i + 1]));
    }

    // Append to stream
    stream->entries.push_back(std::move(entry));

    // Return the newly added ID as a RESP bulk string
    std::string resp = "$" + std::to_string(id_sv.size()) + "\r\n" +
                       std::string(id_sv) + "\r\n";
    send(client_fd, resp.data(), resp.size(), 0);
}

// ============================
// Additional commands would follow the same pattern
// ============================
