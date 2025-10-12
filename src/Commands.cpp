#include "Commands.hpp"
#include "BlockingManager.hpp"
#include <sys/socket.h>
#include <unordered_map>
#include <string>
#include <string_view>
#include <vector>
#include <memory>
#include <chrono>
#include <algorithm>
#include <cstring>

// ============================
// SEND HELPER
// ============================
// Wrapper to send RESP strings safely.
inline void send_response(int client_fd, const char *resp)
{
    send(client_fd, resp, strlen(resp), 0);
}

inline void send_response(int client_fd, std::string_view resp)
{
    send(client_fd, resp.data(), resp.size(), 0);
}

// ============================
// PING
// ============================
void handle_ping(int client_fd)
{
    send_response(client_fd, RESP_PONG);
}

// ============================
// ECHO
// ============================
void handle_echo(int client_fd, const std::vector<std::string_view> &parts)
{
    if (parts.size() < 2)
    {
        send_response(client_fd, RESP_NIL);
        return;
    }

    const auto &val = parts[1];
    std::string resp = "$" + std::to_string(val.size()) + "\r\n" + std::string(val) + "\r\n";
    send_response(client_fd, resp);
}

// ============================
// SET
// ============================
void handle_set(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 3)
    {
        send_response(client_fd, RESP_NIL);
        return;
    }

    const auto key = std::string(parts[1]);
    const auto value = std::string(parts[2]);
    auto expiry = std::chrono::steady_clock::time_point::max();

    // Handle optional PX argument
    if (parts.size() >= 5 && parts[3] == PX_ARG)
    {
        auto px = std::stoll(std::string(parts[4]));
        expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(px);
    }

    kv_store[key] = std::make_unique<StringValue>(value, expiry);
    send_response(client_fd, RESP_OK);
}

// ============================
// GET
// ============================
void handle_get(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 2)
    {
        send_response(client_fd, RESP_NIL);
        return;
    }

    const auto key = std::string(parts[1]);
    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        send_response(client_fd, RESP_NIL);
        return;
    }

    if (auto *str_val = dynamic_cast<StringValue *>(it->second.get()))
    {
        if (str_val->is_expired())
        {
            kv_store.erase(it);
            send_response(client_fd, RESP_NIL);
            return;
        }
        std::string resp = "$" + std::to_string(str_val->value.size()) + "\r\n" + str_val->value + "\r\n";
        send_response(client_fd, resp);
    }
    else
    {
        send_response(client_fd, RESP_NIL);
    }
}

// ============================
// LPUSH
// ============================
void handle_lpush(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 3)
    {
        send_response(client_fd, RESP_NIL);
        return;
    }

    const std::string key(parts[1]);
    ListValue *list_ptr = nullptr;

    if (auto it = kv_store.find(key); it != kv_store.end())
    {
        list_ptr = dynamic_cast<ListValue *>(it->second.get());
        if (!list_ptr)
        {
            send_response(client_fd, RESP_NIL);
            return;
        }
    }
    else
    {
        auto list = std::make_unique<ListValue>();
        list_ptr = list.get();
        kv_store[key] = std::move(list);
    }

    // Push left (front) in order
    for (size_t i = 2; i < parts.size(); ++i)
    {
        list_ptr->values.insert(list_ptr->values.begin(), std::string(parts[i]));
    }

    std::string resp = ":" + std::to_string(list_ptr->values.size()) + "\r\n";
    send_response(client_fd, resp);

    // Unblock any waiting clients
    g_blocking_manager.try_unblock_clients_for_key(key, kv_store,
                                                   [](int fd, const std::string &resp)
                                                   { send(fd, resp.data(), resp.size(), 0); });
}

// ============================
// RPUSH
// ============================
void handle_rpush(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 3)
    {
        send_response(client_fd, RESP_NIL);
        return;
    }

    const std::string key(parts[1]);
    ListValue *list_ptr = nullptr;

    if (auto it = kv_store.find(key); it != kv_store.end())
    {
        list_ptr = dynamic_cast<ListValue *>(it->second.get());
        if (!list_ptr)
        {
            send_response(client_fd, RESP_NIL);
            return;
        }
    }
    else
    {
        auto list = std::make_unique<ListValue>();
        list_ptr = list.get();
        kv_store[key] = std::move(list);
    }

    for (size_t i = 2; i < parts.size(); ++i)
        list_ptr->values.push_back(std::string(parts[i]));

    std::string resp = ":" + std::to_string(list_ptr->values.size()) + "\r\n";
    send_response(client_fd, resp);

    g_blocking_manager.try_unblock_clients_for_key(key, kv_store,
                                                   [](int fd, const std::string &resp)
                                                   { send(fd, resp.data(), resp.size(), 0); });
}

// ============================
// LLEN
// ============================
void handle_llen(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 2)
    {
        send_response(client_fd, RESP_NIL);
        return;
    }

    const auto key = std::string(parts[1]);
    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        send_response(client_fd, ":0\r\n");
        return;
    }

    if (auto *list_val = dynamic_cast<ListValue *>(it->second.get()))
    {
        send_response(client_fd, (":" + std::to_string(list_val->values.size()) + "\r\n").c_str());
    }
    else
    {
        send_response(client_fd, RESP_NIL);
    }
}

// ============================
// LPOP
// ============================
void handle_lpop(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 2)
    {
        send_response(client_fd, RESP_NIL);
        return;
    }

    const std::string key(parts[1]);
    int count = 1;

    if (parts.size() > 2)
    {
        count = std::stoi(std::string(parts[2]));
        if (count < 0)
        {
            send_response(client_fd, "-ERR value is not an integer or out of range\r\n");
            return;
        }
    }

    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        send_response(client_fd, (parts.size() > 2) ? RESP_EMPTY_ARRAY : RESP_NIL);
        return;
    }

    auto *list_val = dynamic_cast<ListValue *>(it->second.get());
    if (!list_val)
    {
        send_response(client_fd, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
        return;
    }
    if (list_val->values.empty())
    {
        send_response(client_fd, (parts.size() > 2) ? RESP_EMPTY_ARRAY : RESP_NIL);
        return;
    }

    size_t elements_to_pop = std::min(static_cast<size_t>(count), list_val->values.size());
    std::string resp;

    if (parts.size() <= 2)
    {
        resp = "$" + std::to_string(list_val->values.front().size()) + "\r\n" + list_val->values.front() + "\r\n";
        list_val->values.erase(list_val->values.begin());
    }
    else
    {
        resp = "*" + std::to_string(elements_to_pop) + "\r\n";
        for (size_t i = 0; i < elements_to_pop; ++i)
            resp += "$" + std::to_string(list_val->values[i].size()) + "\r\n" + list_val->values[i] + "\r\n";
        list_val->values.erase(list_val->values.begin(), list_val->values.begin() + elements_to_pop);
    }

    if (list_val->values.empty())
        kv_store.erase(it);
    send_response(client_fd, resp);
}
// ============================
// LRANGE
// ============================
// LRANGE <key> <start> <stop>
// Returns a RESP array of elements in the specified range.
// Negative indices are supported (e.g., -1 is the last element).
void handle_lrange(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() != 4)
    {
        send_response(client_fd, RESP_ERR_GENERIC);
        return;
    }

    const std::string key(parts[1]);
    int start, stop;

    start = std::stoi(std::string(parts[2]));
    stop = std::stoi(std::string(parts[3]));

    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        send_response(client_fd, RESP_EMPTY_ARRAY);
        return;
    }

    auto *list_val = dynamic_cast<ListValue *>(it->second.get());
    if (!list_val)
    {
        send_response(client_fd, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
        return;
    }

    const auto &values = list_val->values;
    int len = static_cast<int>(values.size());

    // Adjust negative indices
    if (start < 0)
        start = len + start;
    if (stop < 0)
        stop = len + stop;
    start = std::max(0, start);
    stop = std::min(len - 1, stop);

    if (start > stop || start >= len)
    {
        send_response(client_fd, RESP_EMPTY_ARRAY);
        return;
    }

    std::string resp = "*" + std::to_string(stop - start + 1) + "\r\n";
    for (int i = start; i <= stop; ++i)
    {
        resp += "$" + std::to_string(values[i].size()) + "\r\n" + values[i] + "\r\n";
    }

    send_response(client_fd, resp);
}

// ============================
// TYPE
// ============================
void handle_type(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 2)
    {
        send_response(client_fd, "+none\r\n");
        return;
    }

    const auto key = std::string(parts[1]);
    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        send_response(client_fd, "+none\r\n");
        return;
    }

    std::string type_str;
    if (dynamic_cast<StringValue *>(it->second.get()))
        type_str = "string";
    else if (dynamic_cast<ListValue *>(it->second.get()))
        type_str = "list";
    else if (dynamic_cast<StreamValue *>(it->second.get()))
        type_str = "stream";
    else
        type_str = "none";

    send_response(client_fd, ("+" + type_str + "\r\n").c_str());
}

// ============================
// XADD
// ============================
void handle_xadd(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() < 5 || ((parts.size() - 3) % 2 != 0))
    {
        send_response(client_fd, "-ERR wrong number of arguments for 'xadd' command\r\n");
        return;
    }

    const std::string key(parts[1]);
    const std::string_view id_sv = parts[2];

    // Check if full auto-generation is requested
    bool full_auto = (id_sv == "*");
    bool auto_seq = false;
    uint64_t ms = 0;
    uint64_t seq = 0;

    if (!full_auto)
    {
        auto dash = id_sv.find('-');
        if (dash == std::string_view::npos)
        {
            send_response(client_fd, RESP_ERR_GENERIC);
            return;
        }

        // Parse time part
        std::string ms_str(id_sv.substr(0, dash));
        if (ms_str.empty() || ms_str == "*")
        {
            // Partial auto-generation (time part is *) - not supported in this stage
            send_response(client_fd, RESP_ERR_GENERIC);
            return;
        }

        ms = std::stoull(ms_str);

        // Parse sequence part
        std::string seq_str(id_sv.substr(dash + 1));
        if (seq_str == "*")
        {
            auto_seq = true;
            seq = 0; // Will be determined later based on existing entries
        }
        else
        {
            seq = std::stoull(seq_str);
        }

        if (ms == 0 && seq == 0 && !auto_seq)
        {
            send_response(client_fd, RESP_ERR_XADD_ZERO);
            return;
        }
    }

    StreamValue *stream;
    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        auto new_stream = std::make_unique<StreamValue>();
        stream = new_stream.get();
        kv_store[key] = std::move(new_stream);
    }
    else
    {
        stream = dynamic_cast<StreamValue *>(it->second.get());
        if (!stream)
        {
            send_response(client_fd, RESP_ERR_GENERIC);
            return;
        }
    }

    // Handle full auto-generation (*)
    if (full_auto)
    {
        // Get current time in milliseconds
        auto now = std::chrono::system_clock::now();
        auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
        ms = now_ms.time_since_epoch().count();

        if (stream->entries.empty())
        {
            // If no entries, sequence number defaults to 0
            seq = 0;
        }
        else
        {
            const auto &last_id = stream->entries.back().id;
            auto dash_last = last_id.find('-');
            uint64_t last_ms = std::stoull(last_id.substr(0, dash_last));
            uint64_t last_seq = std::stoull(last_id.substr(dash_last + 1));

            if (ms > last_ms)
            {
                // New timestamp, start sequence from 0
                seq = 0;
            }
            else if (ms == last_ms)
            {
                // Same timestamp, increment sequence number
                seq = last_seq + 1;
            }
            else
            {
                // Current time is less than last timestamp - use last timestamp + 1ms
                ms = last_ms + 1;
                seq = 0;
            }
        }
    }
    // Handle auto-sequence number generation only
    else if (auto_seq)
    {
        if (stream->entries.empty())
        {
            // If no entries, sequence number defaults to 0
            // Exception: if time part is 0, sequence number should be 1
            seq = (ms == 0) ? 1 : 0;
        }
        else
        {
            const auto &last_id = stream->entries.back().id;
            auto dash_last = last_id.find('-');
            uint64_t last_ms = std::stoull(last_id.substr(0, dash_last));
            uint64_t last_seq = std::stoull(last_id.substr(dash_last + 1));

            if (ms > last_ms)
            {
                // New timestamp, start sequence from 0
                seq = 0;
            }
            else if (ms == last_ms)
            {
                // Same timestamp, increment sequence number
                seq = last_seq + 1;
            }
            else
            {
                // ms < last_ms - invalid (timestamp must be >= last timestamp)
                send_response(client_fd, RESP_ERR_XADD_EQ);
                return;
            }
        }
    }
    else
    {
        // Validate explicit sequence number
        if (!stream->entries.empty())
        {
            const auto &last_id = stream->entries.back().id;
            auto dash_last = last_id.find('-');
            uint64_t last_ms = std::stoull(last_id.substr(0, dash_last));
            uint64_t last_seq = std::stoull(last_id.substr(dash_last + 1));

            if (ms < last_ms || (ms == last_ms && seq <= last_seq))
            {
                send_response(client_fd, RESP_ERR_XADD_EQ);
                return;
            }
        }
    }

    // Construct the final ID
    std::string final_id = std::to_string(ms) + "-" + std::to_string(seq);

    StreamEntry entry;
    entry.id = final_id;
    entry.fields.reserve((parts.size() - 3) / 2);

    for (size_t i = 3; i + 1 < parts.size(); i += 2)
        entry.fields.emplace(std::string(parts[i]), std::string(parts[i + 1]));

    stream->entries.push_back(std::move(entry));

    std::string resp = "$" + std::to_string(final_id.size()) + "\r\n" + final_id + "\r\n";
    send_response(client_fd, resp);
}

// ============================
// XRANGE (simple)
// ============================
// XRANGE <key> <start> <end>
// Returns an array of entries: [ id, [field1, value1, ...] ]
struct XId
{
    uint64_t ms;
    uint64_t seq;
};

static bool parse_xid_simple(std::string_view sv, bool is_start, XId &out)
{
    try
    {
        auto dash = sv.find('-');
        if (dash == std::string_view::npos)
        {
            // only ms
            out.ms = std::stoull(std::string(sv));
            out.seq = is_start ? 0ULL : UINT64_MAX;
        }
        else
        {
            out.ms = std::stoull(std::string(sv.substr(0, dash)));
            out.seq = std::stoull(std::string(sv.substr(dash + 1)));
        }
        return true;
    }
    catch (...)
    {
        return false;
    }
}

static inline int cmp_xid(const XId &a, const XId &b)
{
    if (a.ms != b.ms)
        return (a.ms < b.ms) ? -1 : 1;
    if (a.seq != b.seq)
        return (a.seq < b.seq) ? -1 : 1;
    return 0;
}

static inline XId parse_entry_id_simple(const std::string &id)
{
    auto dash = id.find('-');
    // Assumes well-formed ids inside the stream
    return XId{std::stoull(id.substr(0, dash)),
               std::stoull(id.substr(dash + 1))};
}

void handle_xrange(int client_fd, const std::vector<std::string_view> &parts, Store &kv_store)
{
    if (parts.size() != 4)
    {
        send_response(client_fd, "-ERR wrong number of arguments for 'xrange'\r\n");
        return;
    }

    const std::string key(parts[1]);
    const auto start_sv = parts[2];
    const auto end_sv = parts[3];

    auto it = kv_store.find(key);
    if (it == kv_store.end())
    {
        send_response(client_fd, RESP_EMPTY_ARRAY);
        return;
    }

    auto *stream = dynamic_cast<StreamValue *>(it->second.get());
    if (!stream)
    {
        send_response(client_fd, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
        return;
    }

    XId start_id{}, end_id{};
    if (!parse_xid_simple(start_sv, /*is_start=*/true, start_id) ||
        !parse_xid_simple(end_sv, /*is_start=*/false, end_id))
    {
        send_response(client_fd, "-ERR invalid stream ID\r\n");
        return;
    }
    if (cmp_xid(start_id, end_id) > 0)
    {
        send_response(client_fd, RESP_EMPTY_ARRAY);
        return;
    }

    // First pass: count matches
    size_t count = 0;
    for (const auto &e : stream->entries)
    {
        XId eid = parse_entry_id_simple(e.id);
        if (cmp_xid(eid, start_id) < 0)
            continue;
        if (cmp_xid(eid, end_id) > 0)
            break;
        ++count;
    }

    // Build RESP
    std::string resp = "*" + std::to_string(count) + "\r\n";
    for (const auto &e : stream->entries)
    {
        XId eid = parse_entry_id_simple(e.id);
        if (cmp_xid(eid, start_id) < 0)
            continue;
        if (cmp_xid(eid, end_id) > 0)
            break;

        resp += "*2\r\n"; // [id, [fields...]]
        resp += "$" + std::to_string(e.id.size()) + "\r\n" + e.id + "\r\n";

        const size_t n = e.fields.size() * 2;
        resp += "*" + std::to_string(n) + "\r\n";
        for (const auto &kv : e.fields)
        {
            resp += "$" + std::to_string(kv.first.size()) + "\r\n" + kv.first + "\r\n";
            resp += "$" + std::to_string(kv.second.size()) + "\r\n" + kv.second + "\r\n";
        }
    }

    send_response(client_fd, resp);
}
