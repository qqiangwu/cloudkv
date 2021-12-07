#include <string_view>
#include <gtest/gtest.h>
#include "memtable/memtable.h"

using namespace std;
using namespace cloudkv;

TEST(memtable, InsertOne)
{
    memtable mt;

    string_view key = "abc";
    string_view val = "val";
    mt.add(key_type::value, key, val);

    auto r = mt.query(key);
    ASSERT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.type(), key_type::value);
    EXPECT_EQ(r.value().value, val);
}

TEST(memtable, Query)
{
    memtable mt;

    const string_view key = "abc";
    const string_view val = "val";
    const string_view val2 = "val2";

    mt.add(key_type::value, key, val);
    auto r = mt.query(key);
    ASSERT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.type(), key_type::value);
    EXPECT_EQ(r.value().value, val);

    mt.add(key_type::value, key, val2);
    r = mt.query(key);
    ASSERT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.type(), key_type::value);
    EXPECT_EQ(r.value().value, val2);
}

TEST(memtable, Delete)
{
    memtable mt;

    string_view key = "abc";
    string_view val = "val";
    mt.add(key_type::value, key, val);
    auto r = mt.query(key);
    ASSERT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.type(), key_type::value);
    EXPECT_EQ(r.value().value, val);

    mt.add(key_type::tombsome, key, "");

    r = mt.query(key);
    ASSERT_TRUE(r);
    EXPECT_EQ(r.value().key.type(), key_type::tombsome);

    mt.add(key_type::value, key, val);
    r = mt.query(key);
    ASSERT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.type(), key_type::value);
    EXPECT_EQ(r.value().value, val);
}

TEST(memtable, QueryRange)
{
    const auto N = 10;

    memtable mt;

    for (int i = 0; i < N; ++i) {
        mt.add(key_type::value, to_string(i), to_string(i));
    }

    auto r = mt.query_range({to_string(1), to_string(5)});
    auto iter = r.begin();
    for (int i = 1; i <= 5; ++i, ++iter) {
        EXPECT_NE(iter, r.end());

        const auto kv = *iter;
        EXPECT_EQ(kv.key.user_key(), to_string(i));
        EXPECT_EQ(kv.key.type(), key_type::value);
        EXPECT_EQ(kv.value, to_string(i));
    }

    for (int i = 0; i < N; i += 2) {
        mt.add(key_type::tombsome, to_string(i), "");
    }

    auto r3 = mt.query_range({to_string(0), to_string(N)});
    int i = 0;
    for (const auto& kv: r3) {
        EXPECT_EQ(kv.key.user_key(), to_string(i));

        if (i % 2 == 0) {
            EXPECT_EQ(kv.key.type(), key_type::tombsome);
        } else {
            EXPECT_EQ(kv.key.type(), key_type::value);
            EXPECT_EQ(kv.value, to_string(i));
        }

        ++i;
    }
}