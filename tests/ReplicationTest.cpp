#include "Replication.hpp"
#include "Commands.hpp"
#include <cassert>
#include <csignal>
#include <chrono>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <iostream>

int main()
{
    const std::string expected_ping = "*1\r\n$4\r\nPING\r\n";
    assert(build_ping_command() == expected_ping);

    const std::string expected_replconf_port = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
    assert(build_replconf_listening_port(6380) == expected_replconf_port);

    const std::string expected_replconf_capa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    assert(build_replconf_capa_psync2() == expected_replconf_capa);

    const std::string expected_psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    assert(build_psync_fullresync() == expected_psync);

    // REPLCONF GETACK should return a RESP array ["REPLCONF", "ACK", "0"].
    std::vector<std::string_view> args = {"REPLCONF", "GETACK", "*"};
    std::string captured;
    ResponseSender capture_sender = [&captured](int, std::string_view resp)
    { captured.assign(resp); };
    handle_replconf(1, args, 0, capture_sender);
    const std::string expected_ack = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n";
    assert(captured == expected_ack);

    // Integration-ish test: spin up a fake master, run our server as a replica, and ensure master receives REPLCONF ACK.
    auto read_until = [](int fd, const std::string &needle, std::string &buf) -> bool
    {
        buf.clear();
        char tmp[512];
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ssize_t n = recv(fd, tmp, sizeof(tmp), 0);
            if (n <= 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            buf.append(tmp, static_cast<size_t>(n));
            if (buf.find(needle) != std::string::npos)
                return true;
        }
        return false;
    };

    const int master_port = 6391;
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(listen_fd >= 0);
    int reuse = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(master_port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    assert(bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
    assert(listen(listen_fd, 1) == 0);

    // Launch replica server process pointing at our fake master.
    pid_t replica_pid = fork();
    assert(replica_pid >= 0);
    if (replica_pid == 0)
    {
        execl("./server", "./server", "--port", "6392", "--replicaof", "127.0.0.1 6391", nullptr);
        _exit(1);
    }

    int conn_fd = accept(listen_fd, nullptr, nullptr);
    assert(conn_fd >= 0);

    std::string buf;
    // Expect PING
    assert(read_until(conn_fd, "PING", buf));
    send(conn_fd, "+PONG\r\n", 7, 0);
    // Expect REPLCONF listening-port
    assert(read_until(conn_fd, "listening-port", buf));
    send(conn_fd, "+OK\r\n", 5, 0);
    // Expect REPLCONF capa psync2
    assert(read_until(conn_fd, "capa", buf));
    send(conn_fd, "+OK\r\n", 5, 0);
    // Expect PSYNC
    assert(read_until(conn_fd, "PSYNC", buf));
    const std::string fullresync = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";
    const auto &rdb = get_empty_rdb_payload();
    std::string bulk = "$" + std::to_string(rdb.size()) + "\r\n";
    bulk.append(rdb);
    send(conn_fd, fullresync.data(), fullresync.size(), 0);
    send(conn_fd, bulk.data(), bulk.size(), 0);

    // Send GETACK and read the replica's ACK.
    const std::string getack = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    send(conn_fd, getack.data(), getack.size(), 0);
    assert(read_until(conn_fd, expected_ack, buf));

    kill(replica_pid, SIGTERM);
    close(conn_fd);
    close(listen_fd);

    return 0;
}
