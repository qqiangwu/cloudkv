#include <cassert>
#include <algorithm>
#include "memtable.h"

using namespace std;
using namespace cloudkv;

class memtable::memtable_iter : public kv_iter {
public:
    explicit memtable_iter(memtable::accessor_t accessor)
        : accessor_(accessor)
    {}

    void seek_first() override
    {
        iter_ = accessor_.begin();
    }

    void seek(std::string_view key) override
    {
        iter_ = accessor_.lower_bound(kv_entry(key));
    }

    bool is_eof() override
    {
        return iter_ == accessor_.end();
    }

    void next() override
    {
        ++iter_;
    }

    key_value_pair current() override
    {
        assert(!is_eof());

        return { iter_->ikey.underlying_key(), iter_->value };
    }

private:
    memtable::accessor_t accessor_;
    memtable::accessor_t::iterator iter_;
};

void memtable::add(key_type op, user_key_ref key, string_view value)
{
    std::uint64_t old_kv_size = 0;
    std::uint64_t new_kv_size = key.size() + 1 + value.size();

    accessor_t accessor(&map_);
    auto it = accessor.find(kv_entry(key));
    if (it != accessor.end()) {
        old_kv_size = it->ikey.underlying_key().size() + it->value.size();

        it->ikey = { key, op };
        it->value = value;
    } else {
        accessor.insert(kv_entry(op, key, value));
    }

    bytes_used_ += new_kv_size;
    bytes_used_ -= old_kv_size;
}

std::optional<internal_key_value> memtable::query(user_key_ref key)
{
    accessor_t accessor(&map_);
    auto it = accessor.find(kv_entry(key));
    if (it == accessor.end()) {
        return std::nullopt;
    }

    return {{ it->ikey, it->value }};
}

iter_ptr memtable::iter()
{
    return std::make_unique<memtable_iter>(accessor_t(&map_));
}