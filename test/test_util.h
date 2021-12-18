#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <range/v3/view.hpp>
#include <range/v3/action.hpp>
#include <range/v3/to_container.hpp>
#include "sstable/sstable.h"
#include "sstable/sstable_builder.h"
#include "util/fmt_std.h"
#include "kv_format.h"

namespace fs = std::filesystem;

struct DBCleaner
{
    const fs::path path_;

    DBCleaner(const fs::path& p)
        : path_(p)
    {
        fs::remove_all(path_);
    }

    ~DBCleaner()
    {
        fs::remove_all(path_);
    }
};

struct ScopedTmpDir
{
    const fs::path path;

    explicit ScopedTmpDir(const char* dir)
        : path(fs::temp_directory_path() / fmt::format("tmp.{}", dir))
    {
        fs::remove_all(path);
        fs::create_directory(path);
    }

    ~ScopedTmpDir()
    {
        fs::remove_all(path);
    }
};

void create_file(const fs::path& p, const std::string& data = {})
{
    std::ofstream ofs(p);

    ofs.write(data.data(), data.size());
    EXPECT_TRUE(ofs);
}

auto make_sst_in_kv_format(const cloudkv::path_t& p, int from = 0, int count = 10)
{
    using namespace cloudkv;
    using namespace ranges;

    sstable_builder builder(options{}, p);
    auto keys = views::ints(from, from + count) | views::transform([](int x){
            return "key-" + std::to_string(x);
        })
        | to<std::vector>
        | actions::sort;

    std::string buf;
    for (const auto& key: keys) {
        internal_key(key, key_type::value).encode_to(&buf);

        builder.add(buf, fmt::format("val-{}-{}", key, p.filename().c_str()));

        buf.clear();
    }

    builder.done();
    return std::make_shared<sstable>(p);
}

auto make_sst(const cloudkv::path_t& p, const std::map<std::string, std::string> kv)
{
    using namespace cloudkv;

    sstable_builder builder(options{}, p);
    for (const auto& [k, v]: kv) {
        builder.add(k, v);
    }

    builder.done();
    return std::make_shared<sstable>(p);
}

auto make_kv(int count)
{
    std::map<std::string, std::string> r;

    for (const auto i: ranges::views::indices(count)) {
        r.emplace(fmt::format("key-{}", i), fmt::format("value-{}", i));
    }

    return r;
}