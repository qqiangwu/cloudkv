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

    size_in_bytes_ = fsize;
}

optional<internal_key_value> sstable::query(user_key_ref key)
{
    auto r = query_range(key, key);
    auto it = ranges::find_if(r, [key](const auto& x){
        return x.key.user_key() == key;
    });
    return it == r.end()? nullopt: optional{ std::move(*it) };
}

vector<internal_key_value> sstable::query_range(std::string_view start_key, std::string_view end_key)
{
    std::ifstream ifs(path_, std::ios::binary);
    if (!ifs) {
        throw_system_error(fmt::format("open sst {} failed", path_));
    }

    ifs.exceptions(std::ios::failbit | std::ios::badbit);

    vector<internal_key_value> results;

    for (size_t i = 0; i < count_; ++i) {
        auto kv = read_kv_(ifs);
        results.push_back(std::move(kv));
    }

    return results;
}

std::string sstable::read_str_(std::istream& in)
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
T sstable::read_int_(std::istream& in)
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

internal_key_value sstable::read_kv_(std::istream& in)
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