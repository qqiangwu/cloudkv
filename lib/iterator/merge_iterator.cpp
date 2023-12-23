#include <cassert>
#include "iterator/merge_iterator.h"

using namespace cloudkv;

void merge_iterator::seek_first()
{
    for (auto& it: iterators_) {
        it->seek_first();
    }

    update_current_();
}

void merge_iterator::seek(std::string_view key)
{
    for (auto& it: iterators_) {
        it->seek(key);
    }

    update_current_();
}

bool merge_iterator::is_eof()
{
    return current_ == nullptr;
}

void merge_iterator::next()
{
    assert(!is_eof());
    assert(current_);

    for (auto& it: iterators_) {
        if (it.get() == current_ || it->is_eof()) {
            continue;
        }

        auto current = internal_key::parse(current_->current().key);
        auto it_key = internal_key::parse(it->current().key);
        if (current.user_key() == it_key.user_key()) {
            it->next();
        }
    }

    current_->next();
    update_current_();
}

merge_iterator::key_value_pair merge_iterator::current()
{
    assert(current_);
    assert(!current_->is_eof());
    return current_->current();
}

void merge_iterator::update_current_()
{
    kv_iter* smallest = nullptr;

    for (const auto& it: iterators_) {
        if (it->is_eof()) {
            continue;
        }

        if (!smallest) {
            smallest = it.get();
        } else {
            // fixme
            auto current = internal_key::parse(smallest->current().key);
            auto it_key = internal_key::parse(it->current().key);

            // for same value, we favor the later iterator
            if (it_key.user_key() <= current.user_key()) {
                smallest = it.get();
            }
        }
    }

    current_ = smallest;
}