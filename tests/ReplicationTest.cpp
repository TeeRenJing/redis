#include "Replication.hpp"
#include <cassert>
#include <string>

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

    return 0;
}
