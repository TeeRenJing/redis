#include "RESP.hpp"
#include <string>

std::vector<std::string_view> parse_resp(std::string_view request) {
    std::vector<std::string_view> parts;
    size_t pos = 0;
    if (!request.empty() && request[pos] == '*') {
        pos = request.find("\r\n", pos);
        if (pos == std::string::npos) return parts;
        pos += 2;
        while (pos < request.size() && request[pos] == '$') {
            size_t len_end = request.find("\r\n", pos);
            if (len_end == std::string::npos) break;
            int len = std::stoi(std::string(request.substr(pos + 1, len_end - pos - 1)));
            pos = len_end + 2;
            if (pos + len > request.size()) break;
            parts.emplace_back(request.substr(pos, len));
            pos += len + 2;
        }
    }
    return parts;
}