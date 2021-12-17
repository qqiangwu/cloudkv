#pragma once

#include <string_view>
#include <memory>
#include "core.h"

namespace cloudkv {

class kv_iter : private boost::noncopyable {
public:
    virtual ~kv_iter() = default;

    virtual void seek(user_key_ref key) = 0;

    virtual bool is_eof() = 0;
    virtual internal_key_value next() = 0;
};

class raw_kv_iter : private boost::noncopyable {
public:
    struct raw_kv {
        std::string_view key;
        std::string_view value;
    };

    virtual ~raw_kv_iter() = default;

    virtual void seek(user_key_ref key) = 0;

    virtual bool is_eof() = 0;

    // @pre !is_eof()
    // the returned string_view keep valid until the iterator is mutated
    virtual raw_kv next() = 0;
};

using iter_ptr = std::unique_ptr<kv_iter>;
using raw_iter_ptr = std::unique_ptr<raw_kv_iter>;

}