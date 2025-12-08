#include "Replication.hpp"

std::string build_resp_array(const std::vector<std::string> &parts)
{
    std::string resp = "*" + std::to_string(parts.size()) + "\r\n";
    for (const auto &part : parts)
    {
        resp += "$" + std::to_string(part.size()) + "\r\n";
        resp += part;
        resp += "\r\n";
    }
    return resp;
}

std::string build_ping_command()
{
    return build_resp_array({"PING"});
}

std::string build_replconf_listening_port(int port)
{
    return build_resp_array({"REPLCONF", "listening-port", std::to_string(port)});
}

std::string build_replconf_capa_psync2()
{
    return build_resp_array({"REPLCONF", "capa", "psync2"});
}
