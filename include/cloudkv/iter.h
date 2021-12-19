#pragma once

#include <string_view>
#include <memory>
#include <boost/noncopyable.hpp>

namespace cloudkv {

// you should use iterator after seek
class kv_iter : private boost::noncopyable {
public:
    virtual ~kv_iter() = default;

    virtual void seek_first() = 0;
    virtual void seek(std::string_view key) = 0;

    virtual bool is_eof() = 0;
    virtual void next() = 0;

    // keep valid until any mutation is performed
    struct key_value_pair {
        std::string_view key;
        std::string_view value;
    };
    virtual key_value_pair current() = 0;
};

using iter_ptr = std::unique_ptr<kv_iter>;

}