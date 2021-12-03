#include <algorithm>
#include "memtable.h"

using namespace std;
using namespace cloudkv;

void memtable::add(key_type op, user_key_ref key, string_view value)
{
    lock_guard _(mut_);

    const auto it = map_.find(key);
    if (it != map_.end()) {
        bytes_used_ -= it->first.user_key().size() + it->second.size();

        map_.erase(it);
    }

    map_.emplace(internal_key{key, op}, value);
    bytes_used_ += key.size();
}

std::optional<internal_key_value> memtable::query(user_key_ref key) const
{
    lock_guard _(mut_);

    auto iter = map_.find(key);
    if (iter == map_.end()) {
        return nullopt;
    }

    return { { iter->first, iter->second } };
}

std::vector<internal_key_value> memtable::query_range(const key_range& r) const
{
    vector<internal_key_value> results;

    if (!(r.start < r.end)) {
        return results;
    }

    lock_guard _(mut_);
    auto lowbound = map_.lower_bound(r.start);
    auto upbound = map_.upper_bound(r.end);

    for_each(lowbound, upbound, [&results](const auto& kv){
        results.push_back({ kv.first, kv.second });
    });

    return results;
}

std::uint64_t memtable::bytes_used() const noexcept
{
    lock_guard _(mut_);
    return bytes_used_;
}