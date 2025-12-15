#pragma once

#include "ReplicationState.hpp"
#include <chrono>
#include <functional>
#include <string>
#include <vector>

// Manages WAIT requests and completes them when enough replicas acknowledge or timeout expires.
class WaitManager
{
public:
    explicit WaitManager(ReplicationTracker &tracker)
        : tracker_(tracker) {}

    void handle_wait(int client_fd, int required, std::chrono::milliseconds timeout,
                     long long current_offset, int connected_replicas,
                     const std::function<void()> &send_getack,
                     const std::function<void(int, const std::string &)> &send_response);

    void process(const std::function<void(int, const std::string &)> &send_callback,
                 const std::function<bool(int)> &client_exists);

    long long last_observed_offset() const { return last_wait_observed_offset_; }
    void note_observed_offset(long long offset) { last_wait_observed_offset_ = offset; }

private:
    struct WaitRequest
    {
        int client_fd;
        int required;
        long long target_offset;
        std::chrono::steady_clock::time_point deadline;
        bool completed = false;
    };

    ReplicationTracker &tracker_;
    std::vector<WaitRequest> requests_;
    long long last_wait_observed_offset_{0};
};
