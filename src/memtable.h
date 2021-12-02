#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <utility>
#include <vector>
#include <memory>
#include <optional>
#include "core.h"

namespace cloudkv {

class memtable {
public:
    void add(user_key_ref key, std::string_view value);

    void remove(user_key_ref key);

    std::optional<internal_key_value> query(user_key_ref key) const;

    std::vector<internal_key_value> query_range(const key_range& r) const; 

    // todo: refactor
    const auto& items() const
    {
        return map_;
    }

    std::uint64_t bytes_used() const noexcept;

private:
    mutable std::mutex mut_;

    std::uint64_t bytes_used_ = 0;
    std::string key_min_;
    std::string key_max_;
    std::map<internal_key, std::string, detail::key_cmp> map_;
};

using memtable_ptr = std::shared_ptr<memtable>;

}