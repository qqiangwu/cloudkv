#include <string_view>
#include <gtest/gtest.h>
#include "memtable/memtable.h"
#include "test_util.h"

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
    auto kv = make_kv(1024, 2);

    memtable mt;
    for (const auto& [k ,v]: kv) {
        mt.add(key_type::value, k, v);
    }

    auto kv_it = kv.begin();
    auto it = mt.iter();
    for (it->seek_first(); !it->is_eof(); it->next(), ++kv_it) {
        ASSERT_TRUE(kv_it != kv.end());

        const auto entry = it->current();
        const auto ikey = internal_key::parse(entry.key);

        ASSERT_EQ(entry.value, kv_it->second);
        ASSERT_EQ(ikey.user_key(), kv_it->first);
        ASSERT_TRUE(ikey.is_value());
    }

    ASSERT_TRUE(kv_it == kv.end());
}

TEST(memtable, IterSeek)
{
    memtable mt;
    mt.add(key_type::value, "1", "1");
    mt.add(key_type::value, "2", "2");
    mt.add(key_type::value, "5", "5");
    mt.add(key_type::value, "7", "7");

    auto it = mt.iter();
    it->seek("2");
    ASSERT_TRUE(!it->is_eof());
    ASSERT_EQ(internal_key::parse(it->current().key).user_key(), "2");
    ASSERT_EQ(it->current().value, "2");

    it->seek("3");
    ASSERT_TRUE(!it->is_eof());
    ASSERT_EQ(internal_key::parse(it->current().key).user_key(), "5");
    ASSERT_EQ(it->current().value, "5");

    it->seek("8");
    ASSERT_TRUE(it->is_eof());
}