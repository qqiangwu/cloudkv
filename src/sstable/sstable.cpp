#include <string_view>
#include <fstream>
#include <filesystem>
#include <fmt/core.h>
#include "cloudkv/exception.h"
#include "sstable/format.h"
#include "sstable/sstable.h"
#include <iostream>

using namespace std;
using namespace cloudkv;

namespace fs = filesystem;

namespace fmt {
template <>
struct formatter<std::filesystem::path> : formatter<std::string> {};
}

sstable::sstable(const path_t& file)
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
}

optional<internal_key_value> sstable::query(std::string_view key)
{
    return {};
}

vector<internal_key_value> sstable::query_range(std::string_view start_key, std::string_view end_key)
{
    return {};
}