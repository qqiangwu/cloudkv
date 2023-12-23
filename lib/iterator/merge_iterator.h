#pragma once

#include <vector>
#include "cloudkv/iter.h"
#include "kv_format.h"

/**
 * @brief merge multiple internal iterators, eg. for compaction
 *
 * if multiple iterators have the same value, the iterator comes first win
 *
 * for simplicity, use O(N) now
 */
namespace cloudkv {

// todo: use heap sort
class merge_iterator : public kv_iter {
public:
    explicit merge_iterator(std::vector<iter_ptr> iterators)
        : iterators_(std::move(iterators))
    {}

    void seek_first() override;

    void seek(std::string_view key) override;

    bool is_eof() override;

    void next() override;

    key_value_pair current() override;

private:
    void update_current_();

private:
    std::vector<iter_ptr> iterators_;
    kv_iter* current_ = nullptr;
};

}