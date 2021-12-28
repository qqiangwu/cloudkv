#include <cassert>
#include <cstdint>
#include <range/v3/empty.hpp>
#include "cloudkv/write_batch.h"
#include "cloudkv/exception.h"
#include "util/format.h"
#include "kv_format.h"

namespace cloudkv {

class write_batch_accessor {
public:
    enum { size_padding = sizeof(std::uint32_t) };

    explicit write_batch_accessor(write_batch& batch)
        : buf_(batch.buf_)
    {
    }

    void add(std::string_view key, std::string_view value)
    {
        add_(key_type::value, key, value);
    }

    void remove(std::string_view key)
    {
        add_(key_type::tombsome, key, "");
    }

    void append(const write_batch& batch)
    {
        assert(buf_.size() >= size_padding);
        assert(batch.buf_.size() >= size_padding);

        EncodeFixed32(buf_.data(), size(batch) + size());

        const auto& other = batch.buf_;
        buf_.append(other.begin() + size_padding, other.end());
    }

    std::string_view to_bytes() const
    {
        assert(buf_.size() >= size_padding);

        return buf_;
    }

    std::uint32_t size() const
    {
        assert(buf_.size() >= size_padding);
        return DecodeFixed32(buf_.data());
    }

    static std::string_view as_bytes(const write_batch& batch)
    {
        assert(batch.buf_.size() >= size_padding);

        return batch.buf_;
    }

    static std::uint32_t size(const write_batch& batch)
    {
        assert(batch.buf_.size() >= size_padding);

        return DecodeFixed32(batch.buf_.data());
    }

    template <class Fn>
    static void iterate(std::string_view buf, Fn fn)
    {
        assert(buf.size() >= size_padding);

        buf.remove_prefix(size_padding);

        try {
            while (!buf.empty()) {
                std::string_view key;
                std::string_view val;

                buf = decode_str(buf, &key);
                buf = decode_str(buf, &val);
                if (key.size() <= 1) {
                    throw data_corrupted{ "corrupted key" };
                }

                const char raw_op = key.back();
                if (raw_op > static_cast<char>(key_type::tombsome)) {
                    throw data_corrupted{ "corrupted key: invalid tag" };
                }

                const auto op = static_cast<key_type>(raw_op);
                key.remove_suffix(1);

                fn(op, key, val);
            }
        } catch (std::invalid_argument&) {
            throw data_corrupted{ "corrupted key-value" };
        }
    }

    template <class Fn>
    static void iterate(const write_batch& batch, Fn fn)
    {
        return iterate(batch.buf_, fn);
    }

private:
    void add_(key_type op, std::string_view key, std::string_view value)
    {
        assert(buf_.size() >= size_padding);

        const auto kv_size = key.size() + value.size() + sizeof(std::uint32_t) * 2 + 1;

        buf_.reserve(buf_.size() + kv_size);
        encode_internel_key(key, op, &buf_);
        encode_str(&buf_, value);

        EncodeFixed32(buf_.data(), size() + 1);
    }

private:
    std::string& buf_;
};

}