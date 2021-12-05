#include <string_view>
#include <fstream>
#include <filesystem>
#include <fmt/core.h>
#include <range/v3/algorithm.hpp>
#include "cloudkv/exception.h"
#include "sstable/format.h"
#include "sstable/sstable.h"
#include "util/fmt_std.h"

using namespace std;
using namespace cloudkv;

namespace fs = filesystem;

class sstable::sstable_iter : public kv_iter {
public:
    explicit sstable_iter(const path_t& path, std::uint64_t payload_len);

    void seek(user_key_ref key) override;

    bool is_eof() override;
    internal_key_value next() override;

private:
    std::string read_str_(std::istream& in);

    template <class T>
    T read_int_(std::istream& in);

    internal_key_value read_kv_(std::istream& in);

private:
    std::ifstream in_;
    const std::uint64_t payload_len_;
};

std::string sstable::sstable_iter::read_str_(std::istream& in)
{
    string buf;
    buf.resize(str_prefix_size);
    in.read(buf.data(), str_prefix_size);

    const auto str_size = DecodeFixed32(buf.data());
    buf.resize(str_size);
    in.read(buf.data(), str_size);

    return buf;
}

template <class T>
T sstable::sstable_iter::read_int_(std::istream& in)
{
    static_assert(sizeof(T) == sizeof(uint64_t) || sizeof(T) == sizeof(uint32_t));

    char buf[sizeof(T)];

    in.read(buf, sizeof(buf));
    if constexpr (sizeof(buf) == sizeof(std::uint64_t)) {
        return DecodeFixed64(buf);
    } else {
        return DecodeFixed32(buf);
    }
}

internal_key_value sstable::sstable_iter::read_kv_(std::istream& in)
{
    auto key = read_str_(in);
    auto raw_type = read_int_<uint32_t>(in);
    auto val = read_str_(in);

    if (raw_type > uint32_t(key_type::tombsome)) {
        throw data_corrupted{ fmt::format("invalid key type {} founnd", raw_type) };
    }

    auto type = key_type(raw_type);
    return { internal_key(key, type), val };
}

sstable::sstable_iter::sstable_iter(const path_t& path, std::uint64_t payload_len)
    : in_(path, std::ios::binary), payload_len_(payload_len)
{
    if (!in_) {
        throw db_corrupted{ fmt::format("open sst {} failed", path) };
    }

    in_.exceptions(std::ios::failbit | std::ios::badbit);
}

// fixme: now we can only seek once
void sstable::sstable_iter::seek(user_key_ref key)
{
    while (!is_eof()) {
        const auto saved_pos = in_.tellg();
        auto kv = next();
        if (kv.key.user_key() >= key) {
            in_.seekg(saved_pos);
            break;
        }
    }
}

bool sstable::sstable_iter::is_eof()
{
    return in_.tellg() >= payload_len_;
}

internal_key_value sstable::sstable_iter::next()
{
    assert(!is_eof());
    return read_kv_(in_);
}

sstable::sstable(const path_t& file)
    : path_(file)
{
    std::ifstream ifs(file, std::ios::binary);
    if (!ifs) {
        throw_system_error(fmt::format("open sst {} failed", file));
    }

    ifs.exceptions(std::ios::failbit | std::ios::badbit);

    const int64_t fsize = fs::file_size(file);
    if (fsize < sst_foot_size) {
        throw data_corrupted{ fmt::format("sst {} too small", file) };
    }

    std::string buf;
    buf.resize(sst_foot_size);
    ifs.seekg(fsize - sst_foot_size);
    ifs.read(buf.data(), buf.size());

    const int64_t real_foot_size = DecodeFixed32(buf.c_str());
    if (fsize < real_foot_size + sst_foot_size) {
        throw data_corrupted{ fmt::format("sst {} too small", file) };
    }

    buf.resize(real_foot_size);
    ifs.seekg(fsize - (real_foot_size + sst_foot_size));
    ifs.read(buf.data(), buf.size());

    try {
        string_view p = buf;
        p = decode_str(p, &key_min_);
        p = decode_str(p, &key_max_);
        if (p.size() < sizeof(uint32_t)) {
            throw data_corrupted{ fmt::format("sst {} footer has no key_count", file) };
        }
        count_ = DecodeFixed32(p.data());
    } catch (std::invalid_argument&) {
        throw data_corrupted{ fmt::format("sst {} footer is of bad format", file) };
    }

    size_in_bytes_ = fsize - (real_foot_size + sst_foot_size);
}

optional<internal_key_value> sstable::query(user_key_ref key)
{
    auto it = iter();
    it->seek(key);
    if (it->is_eof()) {
        return nullopt;
    }

    auto kv = it->next();
    return kv.key.user_key() == key? optional(std::move(kv)): nullopt;
}

iter_ptr sstable::iter()
{
    return std::make_unique<sstable_iter>(path_, size_in_bytes_);
}