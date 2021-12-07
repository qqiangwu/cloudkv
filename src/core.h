#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <filesystem>
#include <tuple>
#include <boost/noncopyable.hpp>

namespace cloudkv {

using boost::noncopyable;

using path_t = std::filesystem::path;
using user_key_ref = std::string_view;

struct key_range {
    user_key_ref start;
    user_key_ref end;
};

enum class key_type : std::uint32_t {
    value,
    tombsome
};

class internal_key {
public:
    internal_key() = default;

    internal_key(user_key_ref key, key_type type)
        : user_key_(key), type_(type)
    {}

    user_key_ref user_key() const
    {
        return user_key_;
    }

    key_type type() const
    {
        return type_;
    }

    bool is_value() const
    {
        return type_ == key_type::value;
    }

    bool is_deleted() const
    {
        return type_ == key_type::tombsome;
    }

    bool operator==(const internal_key& other) const
    {
        return user_key_ == other.user_key_
            && type_ == other.type_;
    }

private:
    std::string user_key_;
    key_type type_;
};

struct internal_key_value {
    internal_key key;
    std::string value;
};

}