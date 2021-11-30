#include <string_view>
#include <gtest/gtest.h>
#include <sstream>
#include <fstream>
#include <vector>
#include <filesystem>
#include <range/v3/view.hpp>
#include <range/v3/action.hpp>
#include <fmt/core.h>
#include "cloudkv/exception.h"
#include "sstable/sstable.h"
#include "sstable/sstable_builder.h"

using namespace std;
using namespace cloudkv;
using namespace ranges;

namespace fs = std::filesystem;

TEST(sstable_builder, Build)
{
    std::ostringstream out;
    sstable_builder builder{out};

    auto raw_keys = views::ints(0, 1000);
    auto keys = raw_keys | views::transform([](int i){ return std::to_string(i); }) | to<std::vector>() | actions::sort;
    auto vals = keys | views::transform([](const auto& k){ return "val-" + k; }) | to<std::vector>();

    for (const auto i: raw_keys) {
        const auto& key = keys[i];
        const auto& val = vals[i];
        builder.add(internal_key{key, seq_number(i), key_type::value}, val);
    }

    builder.done();

    const auto buf = out.str();
    EXPECT_TRUE(!buf.empty());

    const string_view key_min = keys.front();
    const string_view key_max = keys.back();

    const auto tmpfile = fs::temp_directory_path() / "sst";
    {
        std::ofstream ofs(tmpfile, std::ios::trunc);
        ASSERT_TRUE(ofs.is_open());

        ofs.write(buf.c_str(), buf.size());
        ASSERT_TRUE(ofs);
    }

    sstable sst(tmpfile);
    EXPECT_EQ(sst.count(), keys.size());
    EXPECT_EQ(sst.min(), key_min);
    EXPECT_EQ(sst.max(), key_max);

/*
    auto kv = sst.query_range(key_min, key_max);
    EXPECT_EQ(kv.size(), keys.size());

    const auto user_keys = kv | views::transform([](const auto& x){
        return x.key.user_key();
    });
    const auto user_vals = kv | views::transform([](const auto& x){
        return x.value();
    });

    EXPECT_EQ(keys, user_keys);
    EXPECT_EQ(vals, user_vals);*/
}