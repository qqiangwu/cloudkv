#pragma once

#include <cstddef>
#include <vector>
#include <memory>
#include <optional>
#include <istream>
#include "core.h"

namespace cloudkv {

// todo: how to test without virtual methods?
class sstable {
public:
    explicit sstable(const path_t& file);

    std::optional<internal_key_value> query(user_key_ref key);
    std::vector<internal_key_value> query_range(user_key_ref start_key, user_key_ref end_key);

    // TODO add iterator interface

    user_key_ref min() const
    {
        return key_min_;
    }

    user_key_ref max() const
    {
        return key_max_;
    }

    std::size_t count() const
    {
        return count_;
    }

    std::uint64_t size_in_bytes() const
    {
        return size_in_bytes_;
    }

    const path_t& path() const
    {
        return path_;
    }

private:
    std::string read_str_(std::istream& in);

    template <class T>
    T read_int_(std::istream& in);

    internal_key_value read_kv_(std::istream& in);

private:
    path_t path_;

    std::string key_min_;
    std::string key_max_;
    std::size_t count_ = 0;
    std::uint64_t size_in_bytes_ = 0;
};

using sstable_ptr = std::shared_ptr<sstable>;

}