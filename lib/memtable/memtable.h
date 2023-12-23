#pragma once

#include <cstdint>
#include <map>
#include <atomic>
#include <utility>
#include <vector>
#include <memory>
#include <optional>
#include <folly/ConcurrentSkipList.h>
#include "kv_format.h"
#include "cloudkv/iter.h"

namespace cloudkv {

class memtable {
public:
    static constexpr std::uint64_t invalid_logfile_id = 0;

    explicit memtable(std::uint64_t logfile_id = invalid_logfile_id)
        : logfile_id_(logfile_id)
    {
    }

    void add(key_type op, user_key_ref key, std::string_view value);

    std::optional<internal_key_value> query(user_key_ref key);

    iter_ptr iter();

    std::uint64_t logfile_id() const noexcept
    {
        return logfile_id_;
    }

    std::uint64_t bytes_used() const noexcept
    {
        return bytes_used_.load(std::memory_order_relaxed);
    }

private:
    struct kv_entry {
        internal_key ikey;
        std::string value;
        std::string_view ukey;

        kv_entry() = default;  // required by skip_list_t

        explicit kv_entry(user_key_ref key)
            : ukey(key)
        {
        }

        kv_entry(key_type op, user_key_ref key, std::string_view val)
            : ikey{key, op}, value(val)
        {
        }

        user_key_ref user_key() const
        {
            return ukey.empty()? ikey.user_key(): ukey;
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

    class memtable_iter;
    friend memtable_iter;

    const std::uint64_t logfile_id_;

    skip_list_t map_ { 10 };

    std::atomic_uint64_t bytes_used_ { 0 };
};

using memtable_ptr = std::shared_ptr<memtable>;

}