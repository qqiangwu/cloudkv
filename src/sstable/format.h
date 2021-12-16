#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <stdexcept>
#include "core.h"
#include "util/format.h"

namespace cloudkv {

constexpr std::int32_t sst_foot_size = sizeof(std::uint32_t);
constexpr std::int32_t str_prefix_size = sizeof(std::uint32_t);

std::string_view decode_key_type(std::string_view, key_type* type);

}