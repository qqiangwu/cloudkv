#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include "cloudkv/options.h"
#include "core.h"

namespace cloudkv {

/**
 * key1 value1
 * key2 value2
 * ...
 * keyN valueN
 * offset1
 * offset2
 * offset3
 * ...
 * offsetN
 * offset_count
 *
 * basic guarrantee
 */
class block_builder {
public:
    explicit block_builder(const options& opts);

    // todo: does leveldb cope with internal key?
    void add(const internal_key& key, std::string_view value);

    std::string_view done();

    void reset()
    {
        buf_.clear();
        offset_.clear();
    }

    std::uint64_t size_in_bytes() const
    {
        return buf_.size();
    }

private:
    std::string buf_;
    std::vector<std::uint32_t> offset_;
};

}