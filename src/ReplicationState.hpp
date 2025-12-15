#pragma once

#include <string>
#include <unordered_map>

// Tracks replication offsets and per-replica acknowledgements.
class ReplicationTracker
{
public:
    long long current_offset() const { return repl_offset_; }
    void bump_offset(long long delta) { repl_offset_ += delta; }

    void on_replica_connect(int replica_fd, long long offset);
    void on_replica_disconnect(int replica_fd);
    void record_ack(int replica_fd, long long offset);
    int acked_count(long long target_offset) const;

    // Lazily builds and returns the REPLCONF GETACK command.
    const std::string &getack_command();

private:
    long long repl_offset_{0};
    std::unordered_map<int, long long> ack_offsets_;
    std::string cached_getack_;
};
