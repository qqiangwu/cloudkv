#pragma once

#include <cstdint>
#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>
#include "core.h"
#include "sstable/block.h"

namespace cloudkv::cache {

class cache_entry : private noncopyable {
public:
    cache_entry(std::string_view key, block&& b)
        : key_(key), value_(std::move(b))
    {
    }

    std::string_view key() const
    {
        return key_;
    }

    const block& value() const
    {
        return value_;
    }

private:
    const std::string key_;
    block value_;
};

using cache_entry_ptr = std::shared_ptr<cache_entry>;

class simple_cache {
public:
    cache_entry_ptr get(std::string_view key);

    cache_entry_ptr put(std::string_view key, block&& blk);

    void remove(std::string_view key);

private:
    std::mutex mut_;
    std::unordered_map<std::string_view, cache_entry_ptr> cache_;
};

}