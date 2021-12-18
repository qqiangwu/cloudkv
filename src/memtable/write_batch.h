#pragma once

#include <vector>
#include "kv_format.h"

namespace cloudkv {

class write_batch {
public:
    struct single_write {
        std::string_view key;
        std::string_view value;
        key_type op;
    };

    void add(std::string_view key, std::string_view value)
    {
        writes_.push_back({ key, value, key_type::value });
    }

    void remove(std::string_view key)
    {
        writes_.push_back({ key, "", key_type::tombsome });
    }

    void add(const write_batch& other)
    {
        writes_.insert(writes_.end(), other.writes_.begin(), other.writes_.end());
    }

    auto begin() const
    {
        return writes_.begin();
    }

    auto end() const
    {
        return writes_.end();
    }

    auto size() const
    {
        return writes_.size();
    }

private:
    std::vector<single_write> writes_;
};

}