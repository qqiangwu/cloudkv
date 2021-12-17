#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <stdexcept>
#include "core.h"
#include "util/format.h"

namespace cloudkv {

constexpr std::int32_t sst_foot_size = sizeof(std::uint32_t);
constexpr std::int32_t str_prefix_size = sizeof(std::uint32_t);

std::string_view decode_key_type(std::string_view, key_type* type);

namespace sst {

class block_handle {
public:
    enum { block_handle_size = 2 * sizeof(std::uint64_t) };

    block_handle(std::uint64_t offset, std::uint64_t length)
        : offset_(offset), length_(length)
    {
    }

    explicit block_handle(std::string_view content);

    std::uint64_t offset() const
    {
        return offset_;
    }

    std::uint64_t length() const
    {
        return length_;
    }

    void encode_to(std::string* out);

private:
    std::uint64_t offset_ = -1;
    std::uint64_t length_ = -1;
};

/**
 * dataindex block_handle
 * metaindex block_handle
 * magic     uint64_t
 */
class footer {
public:
    enum { table_magic = 2021122501LL };
    enum { footer_size = 2 * block_handle::block_handle_size + sizeof(std::uint64_t) };

    footer(block_handle dataindex, block_handle metaindex)
        : data_index_(dataindex), meta_index_(metaindex)
    {
    }

    explicit footer(std::string_view content);

    block_handle data_index() const
    {
        return data_index_;
    }

    block_handle meta_index() const
    {
        return meta_index_;
    }

    void encode_to(std::string* out);

private:
    block_handle data_index_;
    block_handle meta_index_;
};

}

}