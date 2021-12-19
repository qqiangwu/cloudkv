#include <cassert>
#include "cloudkv/exception.h"
#include "util/fmt_std.h"
#include "util/coding.h"
#include "sstable/format.h"
#include "block.h"

using namespace cloudkv;

namespace {

std::uint32_t parse_count(std::string_view buf)
{
    const auto u32_size = sizeof(std::uint32_t);

    if (buf.size() < u32_size) {
        throw data_corrupted{ fmt::format("invalid block length: {}", buf.size()) };
    }

    return DecodeFixed32(buf.data() + buf.size() - u32_size);
}

class block_iter : public kv_iter {
public:
    block_iter(std::string_view buf, std::uint32_t count)
        : buf_(buf),
          count_(count),
          offset_table_(buf_.data() + buf_.size() - count_ * sizeof(std::uint32_t))
    {
        assert(buf_.size() >= count_ * sizeof(std::uint32_t));
    }

    void seek_first() override
    {
        seek_to_index_(0);
    }

    void seek(std::string_view key) override
    {
        std::uint32_t first = 0;
        std::uint32_t last = count_;
        while (first != last) {
            const auto mid = first + (last - first) / 2;
            const auto key_at_mid = get_key(mid);
            if (key_at_mid < key) {
                first = mid + 1;
            } else {
                last = mid;
            }
        }

        seek_to_index_(first);
    }

    bool is_eof() override
    {
        return idx_ == count_;
    }

    void next() override
    {
        assert(!is_eof());
        seek_to_index_(idx_ + 1);
    }

    key_value_pair current() override
    {
        assert(!is_eof());
        assert(!current_key_.empty());

        return { current_key_, current_value_ };
    }

private:
    std::uint32_t get_offset_(std::uint32_t idx) const
    {
        assert(idx <= count_);

        return DecodeFixed32(offset_table_ + idx * sizeof(std::uint32_t));
    }

    std::string_view get_key(std::uint32_t idx) const
    {
        assert(idx < count_);

        auto offset = get_offset_(idx);
        auto kv = buf_.substr(offset);

        try {
            return decode_str(kv);
        } catch (std::invalid_argument&) {
            throw data_corrupted{ fmt::format("invalid key to decode in idx {}", idx) };
        }
    }

    void seek_to_index_(std::uint32_t idx)
    {
        assert(idx <= count_);

        idx_ = idx;
        if (idx_ == count_) {
            return;
        }

        try {
            const auto offset = get_offset_(idx_);
            std::string_view kvbuf = buf_.substr(offset);

            kvbuf = decode_str(kvbuf, &current_key_);
            kvbuf = decode_str(kvbuf, &current_value_);

            if (kvbuf.data() > offset_table_) {
                throw data_corrupted{ fmt::format("block corrupted when read index {}", idx_ - 1) };
            }
        } catch (std::invalid_argument&) {
            throw data_corrupted{ fmt::format("block corrupted when read index {}", idx_ - 1) };
        }
    }

private:
    const std::string_view buf_;
    const std::uint32_t count_;
    const char* offset_table_;
    std::uint32_t idx_ = 0;

    std::string_view current_key_;
    std::string_view current_value_;
};

}

block::block(std::string_view buf)
    : buf_(buf), count_(parse_count(buf))
{
    if (buf_.size() < count_ * sizeof(std::uint32_t)) {
        throw data_corrupted{ fmt::format("block corrupted, {} keys with only size {}", count_, buf_.size()) };
    }
}

iter_ptr block::iter()
{
    std::string_view buf = buf_;
    buf.remove_suffix(sizeof(std::uint32_t));
    return std::make_unique<block_iter>(buf, count_);
}