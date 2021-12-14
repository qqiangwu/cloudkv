#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <stdexcept>
#include "util/coding.h"

namespace cloudkv {

inline void encode_str(std::string* buf, std::string_view s)
{
    PutFixed32(buf, s.size());
    *buf += s;
}

// str format: fix32 + chars
inline std::string_view decode_str(std::string_view buf, std::string* out)
{
    const auto size_size = sizeof(uint32_t);
    if (buf.size() < size_size) {
        throw std::invalid_argument{ "no str to decode" };
    }

    const auto str_size = DecodeFixed32(buf.data());
    buf.remove_prefix(size_size);

    if (buf.size() < str_size) {
        throw std::invalid_argument{ "no str to decode" };
    }

    out->assign(buf.data(), str_size);

    buf.remove_prefix(str_size);
    return buf;
}

}