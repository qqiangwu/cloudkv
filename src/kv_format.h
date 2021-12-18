#pragma once

#include <cstdint>
#include <string_view>
#include "core.h"

namespace cloudkv {

using user_key_ref = std::string_view;

struct key_range {
    user_key_ref start;
    user_key_ref end;
};

enum class key_type : std::uint8_t {
    value,
    tombsome
};

class internal_key {
public:
    internal_key() = default;

    internal_key(user_key_ref key, key_type type)
        : user_key_(key), type_(type)
    {}

    static internal_key parse(std::string_view buf);

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

    void encode_to(std::string* out) const
    {
        out->append(user_key_.begin(), user_key_.end());
        out->push_back(static_cast<char>(type_));
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