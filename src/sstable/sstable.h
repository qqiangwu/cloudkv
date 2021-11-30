#pragma once

#include <cstddef>
#include <vector>
#include <memory>
#include <optional>
#include "core.h"

namespace cloudkv {

class sstable {
public:
    explicit sstable(const path_t& file);

    std::optional<internal_key_value> query(std::string_view key);
    std::vector<internal_key_value> query_range(std::string_view start_key, std::string_view end_key);

    user_key min() const
    {
        return key_min_;
    }

    user_key max() const
    {
        return key_max_;
    }

    std::size_t count() const
    {
        return count_;
    }

    const path_t& path() const
    {
        return path_;
    }

private:
    path_t path_;

    std::string key_min_;
    std::string key_max_;
    std::size_t count_;
};

using sstable_ptr = std::shared_ptr<sstable>;

}