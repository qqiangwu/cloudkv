#include "core.h"
#include "cloudkv/exception.h"
#include "sstable/format.h"
#include "util/fmt_std.h"

using namespace cloudkv;

std::string_view cloudkv::decode_key_type(std::string_view buf, key_type* type)
{
    if (buf.size() <= 1) {
        throw data_corrupted{ "key length too small" };
    }

    const uint32_t raw_key_type = buf.back();
    if (raw_key_type > uint32_t(key_type::tombsome)) {
        throw data_corrupted{ fmt::format("invalid key type {} founnd", raw_key_type) };
    }

    *type = key_type(raw_key_type);

    buf.remove_prefix(1);
    return buf;
}