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

    user_key min() const;
    user_key max() const;
    std::size_t count() const;

    const path_t& path() const;
};

using sstable_ptr = std::shared_ptr<sstable>;

}