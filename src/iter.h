#pragma once

#include <memory>
#include "core.h"

namespace cloudkv {

// todo: how to define iterator interface
class kv_iter : private boost::noncopyable {
public:
    virtual ~kv_iter() = default;

    virtual void seek(user_key_ref key) = 0;

    virtual bool is_eof() = 0;
    virtual internal_key_value next() = 0;
};

using iter_ptr = std::unique_ptr<kv_iter>;

}