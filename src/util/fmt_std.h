#pragma once

#include <string>
#include <filesystem>
#include <fmt/core.h>

namespace fmt {
template <>
struct formatter<std::filesystem::path> : formatter<std::string> {};
}