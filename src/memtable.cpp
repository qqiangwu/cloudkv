#include <algorithm>
#include "memtable.h"

using namespace std;
using namespace cloudkv;

void memtable::add(string_view key, string_view value, seq_number seq)
{
    lock_guard _(mut_);

    map_.emplace(internal_key{key, seq, key_type::value}, value);
}

void memtable::remove(string_view key, seq_number seq)
{
    lock_guard _(mut_);

    map_.emplace(internal_key{key, seq, key_type::tombsome}, "");
}

std::optional<internal_key_value> memtable::query(lookup_key key) const
{
    lock_guard _(mut_);

    auto iter = map_.upper_bound(key);
    if (iter == map_.begin()) {
        return nullopt;
    }

    --iter;
    if (iter->first.user_key() != key.key()) {
        return nullopt;
    }

    return { { iter->first, iter->second } };
}

std::vector<internal_key_value> memtable::query_range(const key_range& r, seq_number seq) const
{
    lock_guard _(mut_);
    if (!(r.start < r.end)) {
        return {};
    }

    std::vector<internal_key_value> results;
    auto lowbound = map_.upper_bound(lookup_key(r.start, seq));
    auto upbound = map_.upper_bound(lookup_key(r.end, seq));
    if (lowbound != map_.begin() && prev(lowbound)->first.user_key() == r.start) {
        advance(lowbound, -1);
    }

    for_each(lowbound, upbound, [seq, &results](const auto& kv){
        if (kv.first.seq() > seq) {
            return;
        }

        if (results.empty() || results.back().key.user_key() != kv.first.user_key()) {
            results.push_back({kv.first, kv.second});
        }
    });

    return results;
}