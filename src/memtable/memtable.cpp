#include <algorithm>
#include "memtable.h"

using namespace std;
using namespace cloudkv;

void memtable::add(key_type op, user_key_ref key, string_view value)
{
    std::uint64_t old_kv_size = 0;
    std::uint64_t new_kv_size = key.size() + value.size();

    accessor_t accessor(&map_);
    auto it = accessor.find(kv_entry(key));
    if (it != accessor.end()) {
        const auto& kv = it->kv;

        old_kv_size = kv.key.user_key().size() + kv.value.size();
        it->kv.key = { key, op };
        it->kv.value = value;
    } else {
        accessor.insert(kv_entry(op, key, value));
    }

    lock_guard _(mut_);
    bytes_used_ += new_kv_size;
    bytes_used_ -= old_kv_size;
}

std::optional<internal_key_value> memtable::query(user_key_ref key)
{
    accessor_t accessor(&map_);
    auto it = accessor.find(kv_entry(key));
    if (it == accessor.end()) {
        return std::nullopt;
    }

    return it->kv;
}

std::vector<internal_key_value> memtable::query_range(const key_range& r)
{
    vector<internal_key_value> results;

    if (r.start > r.end) {
        return results;
    }

    accessor_t accessor(&map_);
    auto lowbound = accessor.lower_bound(kv_entry(r.start));
    auto upbound = accessor.lower_bound(kv_entry(r.end));
    if (upbound != accessor.end() && upbound->user_key() == r.end) {
        ++upbound;
    }

    for_each(lowbound, upbound, [&results](const auto& entry){
        results.push_back(entry.kv);
    });

    return results;
}

std::uint64_t memtable::bytes_used() const noexcept
{
    lock_guard _(mut_);
    return bytes_used_;
}