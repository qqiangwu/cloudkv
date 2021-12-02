#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <filesystem>
#include <tuple>

namespace cloudkv {

using path_t = std::filesystem::path;
using user_key_ref = std::string_view;

struct key_range {
    user_key_ref start;
    user_key_ref end;
};

enum class key_type {
    value,
    tombsome
};

class internal_key {
public:
    explicit internal_key(std::string_view raw_key);

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

namespace detail {

struct key_cmp {
    using is_transparent = void;

    bool operator()(const internal_key& x, const internal_key& y) const
    {
        return x.user_key() < y.user_key();
    }

    bool operator()(const internal_key& x, user_key_ref y) const
    {
        return x.user_key() < y;
    }

    bool operator()(user_key_ref x, const internal_key& y) const
    {
        return x < y.user_key();
    }
};  

}

}