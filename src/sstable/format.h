#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <stdexcept>
#include "util/format.h"

namespace cloudkv {

constexpr std::int32_t sst_foot_size = sizeof(std::uint32_t);
constexpr std::int32_t str_prefix_size = sizeof(std::uint32_t);

}