#pragma once

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
    void add(std::string_view key, std::string_view value, seq_number seq);

    void remove(std::string_view, seq_number seq);

    std::optional<internal_key_value> query(lookup_key key) const;

    std::vector<internal_key_value> query_range(const key_range& r, seq_number seq) const; 

private:
    struct key_cmp {
        using is_transparent = void;

        bool operator()(const internal_key& x, const internal_key& y) const
        {
            return std::make_tuple(x.user_key(), x.seq(), x.type()) < std::make_tuple(y.user_key(), y.seq(), y.type());
        }

        bool operator()(const internal_key& x, const lookup_key& y) const
        {
            return std::make_tuple(x.user_key(), x.seq()) < std::make_tuple(y.key(), y.seq());
        }

        bool operator()(const lookup_key& x, const internal_key& y) const
        {
            return std::make_tuple(x.key(), x.seq()) < std::make_tuple(y.user_key(), y.seq());
        }
    };

    mutable std::mutex mut_;
    std::map<internal_key, std::string, key_cmp> map_;
};

using memtable_ptr = std::shared_ptr<memtable>;

}