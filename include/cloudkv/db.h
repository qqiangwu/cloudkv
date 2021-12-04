#pragma once

#include <vector>
#include <string_view>
#include <optional>
#include <map>
#include <memory>
#include <boost/noncopyable.hpp>

namespace cloudkv {

struct key_value {
    std::string_view key;
    std::string_view value;
};

struct options {
    bool open_only = false;
    std::uint64_t write_buffer_size = 4 * 1024 * 1024;
    std::uint64_t sstable_size = 4 * 1024 * 1024;
    std::uint64_t compaction_water_mark = 8;
};

class kv_store : private boost::noncopyable {
public:
    virtual ~kv_store() = default;

    virtual std::optional<std::string> query(std::string_view key) = 0;

    // partial inserts may occur
    virtual void batch_add(const std::vector<key_value>& key_values) = 0;
    
    virtual void remove(std::string_view key) = 0;
    
    virtual std::map<std::string, std::string> query_range(
        std::string_view start_key,
        std::string_view end_key, 
        int count = 128) = 0;
};

// return nullptr if not exists
std::unique_ptr<kv_store> open(std::string_view name, const options& opts);

}
