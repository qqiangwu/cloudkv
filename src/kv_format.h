#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace cloudkv {

using user_key_ref = std::string_view;

enum class key_type : std::uint8_t {
    value,
    tombsome
};

class internal_key {
public:
    internal_key() = default;

    internal_key(user_key_ref key, key_type type)
    {
        ikey_.reserve(key.size() + 1);
        ikey_.assign(key.begin(), key.end());
        ikey_.push_back(static_cast<char>(type));
    }

    static internal_key parse(std::string_view buf);

    user_key_ref user_key() const
    {
        return { ikey_.data(), ikey_.size() - 1 };
    }

    key_type type() const
    {
        return static_cast<key_type>(ikey_.back());
    }

    bool is_value() const
    {
        return type() == key_type::value;
    }

    bool is_deleted() const
    {
        return type() == key_type::tombsome;
    }

    bool operator==(const internal_key& other) const
    {
        return ikey_ == other.ikey_;
    }

    std::string_view underlying_key() const
    {
        return ikey_;
    }

private:
    std::string ikey_;
};

struct internal_key_value {
    internal_key key;
    std::string value;
};

}