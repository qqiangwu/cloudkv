#include "blockcache/cache.h"

using namespace cloudkv;
using namespace cloudkv::cache;

cache_entry_ptr simple_cache::get(std::string_view key)
{
    std::lock_guard _(mut_);
    auto it = cache_.find(key);
    return it == cache_.end()? nullptr: it->second;
}

cache_entry_ptr simple_cache::put(std::string_view key, block&& blk)
{
    std::lock_guard _(mut_);
    auto it = cache_.find(key);
    if (it != cache_.end()) {
        return it->second;
    }

    auto e = std::make_shared<cache_entry>(key, std::move(blk));
    cache_.emplace(e->key(), e);
    return e;
}

void simple_cache::remove(std::string_view key)
{
    std::lock_guard _(mut_);
    cache_.erase(key);
}