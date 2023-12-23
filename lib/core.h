#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <filesystem>
#include <tuple>
#include <boost/noncopyable.hpp>

namespace cloudkv {

using boost::noncopyable;

using path_t = std::filesystem::path;

namespace fs = std::filesystem;

}