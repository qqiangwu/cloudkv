#include <string_view>
#include <gtest/gtest.h>
#include <sstream>
#include <vector>
#include <range/v3/view.hpp>
#include <range/v3/action.hpp>
#include "sstable/sstable_builder.h"

using namespace std;
using namespace cloudkv;
using namespace ranges;

TEST(sstable_builder, Build)
{
    std::ostringstream out;
    sstable_builder builder{out};

    auto raw_keys = views::ints(0, 1000);
    auto keys = raw_keys | views::transform([](int i){ return std::to_string(i); }) | to<std::vector>();
    actions::sort(keys);

    auto vals = keys | views::transform([](const auto& k){ return "val-" + k; }) | to<std::vector>();

    for (const auto i: raw_keys) {
        const auto& key = keys[i];
        const auto& val = vals[i];
        builder.add(internal_key{key, seq_number(i), key_type::value}, val);
    }

    builder.done();

    const auto buf = out.str();
    EXPECT_TRUE(!buf.empty());
}