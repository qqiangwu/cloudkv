#pragma once

#include <optional>
#include <vector>
#include "iter.h"
#include "kv_format.h"

namespace cloudkv {

inline std::vector<internal_key_value> dump_all(const iter_ptr& iter)
{
    std::vector<internal_key_value> results;

    for (iter->seek_first(); !iter->is_eof(); iter->next()) {
        auto [k, v] = iter->current();
        results.push_back({internal_key::parse(k), std::string(v)});
    }

    return results;
}

inline std::optional<internal_key_value> query_key(const iter_ptr& iter, std::string_view key)
{
    iter->seek(key);
    if (iter->is_eof()) {
        return {};
    }

    const auto [k, v] = iter->current();
    auto ikey = internal_key::parse(k);
    if (ikey.user_key() != key) {
        return {};
    }

    return std::optional(internal_key_value{ std::move(ikey), std::string(v) });
}

}