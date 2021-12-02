#include <string_view>
#include <gtest/gtest.h>
#include "memtable.h"

using namespace std;
using namespace cloudkv;

TEST(memtable, InsertOne)
{
    memtable mt;

    string_view key = "abc";
    string_view val = "val";
    mt.add(key, val);
    auto r = mt.query(key);
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.type(), key_type::value); 
    EXPECT_EQ(r.value().value, val);
}

TEST(memtable, Query)
{
    memtable mt;

    string_view key = "abc";
    string_view val = "val";
    mt.add(key, val);
    auto r = mt.query(key);
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.type(), key_type::value); 
    EXPECT_EQ(r.value().value, val);
}

TEST(memtable, Delete)
{
    memtable mt;

    string_view key = "abc";
    string_view val = "val";
    mt.add(key, val);
    auto r = mt.query(key);
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.type(), key_type::value); 
    EXPECT_EQ(r.value().value, val);

    mt.remove(key);
    
    r = mt.query(key);
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.type(), key_type::tombsome);
}

TEST(memtable, QueryRange)
{
    const auto N = 10;

    memtable mt;

    for (int i = 0; i < N; ++i) {
        mt.add(to_string(i), to_string(i));
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
        mt.remove(to_string(i));
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