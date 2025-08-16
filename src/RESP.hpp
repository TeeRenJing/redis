#pragma once
#include <string>
#include <vector>
#include <string_view>

std::vector<std::string_view> parse_resp(const std::string_view &request);