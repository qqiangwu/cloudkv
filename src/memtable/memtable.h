#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <utility>
#include <vector>
#include <memory>
#include <optional>
#include <folly/ConcurrentSkipList.h>
#include "core.h"

namespace cloudkv {

class memtable {
public:
    void add(key_type op, user_key_ref key, std::string_view value);

    std::optional<internal_key_value> query(user_key_ref key);

    std::vector<internal_key_value> query_range(const key_range& r);

    // todo: refactor
    auto items()
    {
        return accessor_t(&map_);
    }

    std::uint64_t bytes_used() const noexcept;

private:
    struct kv_entry {
        internal_key_value kv;
        user_key_ref key;

        kv_entry() = default;  // required by skip_list_t

        explicit kv_entry(user_key_ref key)
            : key(key)
        {}

        kv_entry(key_type op, user_key_ref key, std::string_view value)
            : kv{{key, op}, std::string(value)}
        {}

        user_key_ref user_key() const
        {
            return key.empty()? kv.key.user_key(): key;
        }
    };

    struct kv_entry_cmp {
        bool operator()(const kv_entry& x, const kv_entry& y) const
        {
            return x.user_key() < y.user_key();
        }
    };

    using skip_list_t = folly::ConcurrentSkipList<kv_entry, kv_entry_cmp>;
    using accessor_t = skip_list_t::Accessor;

    skip_list_t map_ { 10 };

    mutable std::mutex  mut_;
    std::uint64_t bytes_used_ = 0;
};

using memtable_ptr = std::shared_ptr<memtable>;

}