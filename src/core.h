#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <filesystem>

namespace cloudkv {

using path_t = std::filesystem::path;

using seq_number = std::uint64_t;
using user_key = std::string_view;

struct key_range {
    user_key start;
    user_key end;
};

enum class key_type {
    value,
    tombsome
};

class lookup_key {
public:
    lookup_key(std::string_view key, seq_number seq)
        : key_(key), seq_(seq)
    {}

    std::string_view key() const
    {
        return key_;
    }

    seq_number seq() const
    {
        return seq_;
    }

private:
    std::string_view key_;
    seq_number seq_;
};

class internal_key {
public:
    explicit internal_key(std::string_view raw_key);

    internal_key(std::string_view raw_key, seq_number seq, key_type type)
        : user_key_(raw_key), seq_(seq), type_(type)
    {}

    std::string_view user_key() const
    {
        return user_key_;
    }

    seq_number seq() const
    {
        return seq_;
    }

    key_type type() const
    {
        return type_;
    }

    bool operator==(const internal_key& other) const
    {
        return user_key_ == other.user_key_
            && seq_ == other.seq_
            && type_ == other.type_;
    }

private:
    std::string user_key_;
    seq_number seq_;
    key_type type_;
};

struct internal_key_value {
    internal_key key;
    std::string value;
};

}