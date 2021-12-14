#include <filesystem>
#include <fstream>
#include <range/v3/view.hpp>
#include <range/v3/action.hpp>
#include <range/v3/to_container.hpp>
#include "sstable/sstable.h"
#include "sstable/sstable_builder.h"
#include "util/fmt_std.h"

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

void create_file(const fs::path& p, const std::string& data = {})
{
    std::ofstream ofs(p);

    ofs.write(data.data(), data.size());
    EXPECT_TRUE(ofs);
}

auto make_sst(const cloudkv::path_t& p, int from = 0, int count = 10)
{
    using namespace cloudkv;
    using namespace ranges;

    sstable_builder builder(p);
    auto keys = views::ints(from, from + count) | views::transform([](int x){
            return "key-" + std::to_string(x);
        })
        | to<std::vector>
        | actions::sort;

    for (const auto& key: keys) {
        builder.add(internal_key{ key, key_type::value }, fmt::format("val-{}-{}", key, p.filename().c_str()));
    }

    builder.done();
    return std::make_shared<sstable>(p);
}