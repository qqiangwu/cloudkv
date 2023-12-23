#include "cloudkv/exception.h"
#include "kv_format.h"

using namespace cloudkv;

internal_key internal_key::parse(std::string_view key)
{
    if (key.size() <= 1) {
        throw data_corrupted{ "key is too small" };
    }

    const std::uint8_t keytype = key.back();
    key.remove_suffix(1);

    if (keytype > std::uint8_t(key_type::tombsome)) {
        throw data_corrupted{ "key type corrupted" };
    }

    return { key, static_cast<key_type>(keytype) };
}