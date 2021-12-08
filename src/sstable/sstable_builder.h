#pragma once

#include <cstdint>
#include <optional>
#include <memory>
#include <fstream>
#include "sstable/sstable.h"

namespace cloudkv {

/**
 * @brief simple format: sorted string list
 *
 * [record1]: key_size|key_chars|key_type|value_size|value_chars
 * [record2]
 * [record3]
 * ...
 * [recordN]
 * [footer]
 *      min_key: key_size(fix32)|key_chars
 *      max_key: key_size(fix32)|key_chars
 *      key_value_count: fix32
 *
 */
class sstable_builder {
public:
    // todo: remove this
    explicit sstable_builder(std::ostream& out);
    explicit sstable_builder(const path_t& p);

    void add(const internal_key& key, std::string_view value);

    void done();

    std::uint64_t size_in_bytes() const
    {
        return size_in_bytes_;
    }

    const path_t& target() const
    {
        return path_;
    }

private:
    std::string build_record_(const internal_key& key, std::string_view value);
    std::string build_footer_();

private:
    const path_t path_;
    std::unique_ptr<std::ofstream> buf_;
    std::ostream& out_;

    std::string key_min_;
    std::string key_max_;
    int count_ = 0;
    std::uint64_t size_in_bytes_ = 0;
};

}