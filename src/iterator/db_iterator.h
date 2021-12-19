#pragma once

#include "cloudkv/iter.h"
#include "kv_format.h"

/**
 * @brief iterator wrapper, convert internal key value to use key value
 *
 */
namespace cloudkv {

class db_iterator : public kv_iter {
public:
    explicit db_iterator(iter_ptr iter)
        : internal_iter_(std::move(iter))
    {
    }

    void seek_first() override
    {
        internal_iter_->seek_first();
        skip_deleted_();
    }

    void seek(std::string_view key) override
    {
        internal_iter_->seek(key);
        skip_deleted_();
    }

    bool is_eof() override
    {
        return internal_iter_->is_eof();
    }

    void next() override
    {
        internal_iter_->next();
        skip_deleted_();
    }

    key_value_pair current() override
    {
        auto kv = internal_iter_->current();

        // fixme: so ugly
        // remove tag
        kv.key.remove_suffix(1);

        return kv;
    }

private:
    void skip_deleted_()
    {
        while (!internal_iter_->is_eof()) {
            const auto ikey = internal_key::parse(internal_iter_->current().key);
            if (!ikey.is_deleted()) {
                break;
            }

            internal_iter_->next();
        }
    }

private:
    iter_ptr internal_iter_;
};

}