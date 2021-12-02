#include <string_view>
#include <range/v3/view.hpp>
#include <range/v3/action.hpp>
#include <range/v3/algorithm.hpp>
#include <gtest/gtest.h>
#include "db_impl.h"

using namespace std;
using namespace cloudkv;
using namespace ranges;

TEST(db, Basic)
{
    auto db = open("test", options());
    EXPECT_TRUE(db);

    const string_view key = "123";
    const string_view val = "780";
    const string_view bad_key = "12";

    EXPECT_FALSE(db->query(key));
    EXPECT_FALSE(db->query(bad_key)); 

    db->batch_add({
        { key, val }
    });

    const auto r = db->query(key);
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value(), val);

    EXPECT_FALSE(db->query(bad_key)); 
}

TEST(db, MultiInsert)
{
    auto db = open("test", options());
    EXPECT_TRUE(db);

    auto raw_keys = views::ints(0, 1000);
    auto keys = raw_keys | views::transform([](int i){ return std::to_string(i); }) | to<std::vector>() | actions::sort;
    auto key_values = keys | views::transform([](const auto& k){
        return key_value{k, "val-" + k};
    }) | to<std::vector>();

    db->batch_add(key_values);

    for (const auto& [key, val]: key_values) {
        const auto r = db->query(key);
        EXPECT_TRUE(r);
        EXPECT_EQ(r.value(), val);
    }
}