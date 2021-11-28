#include <string_view>
#include <gtest/gtest.h>
#include <iostream>
#include "memtable.h"

using namespace std;
using namespace cloudkv;

TEST(memtable, InsertOne)
{
    memtable mt;

    seq_number seq = 1;
    string_view key = "abc";
    string_view val = "val";
    mt.add(key, val, seq);
    auto r = mt.query({key, seq});
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.seq(), seq); 
    EXPECT_EQ(r.value().key.type(), key_type::value); 
    EXPECT_EQ(r.value().value, val);
}

TEST(memtable, InsertTwice)
{
    memtable mt;

    seq_number seq = 1;
    string_view key = "abc";
    string_view val = "val";
    string_view val2 = "val2";
    mt.add(key, val, seq);
    mt.add(key, val2, seq + 1);

    auto r = mt.query({key, seq});
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.seq(), seq); 
    EXPECT_EQ(r.value().key.type(), key_type::value); 
    EXPECT_EQ(r.value().value, val);

    r = mt.query({key, seq + 1});
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.seq(), seq + 1); 
    EXPECT_EQ(r.value().key.type(), key_type::value); 
    EXPECT_EQ(r.value().value, val2);
}

TEST(memtable, Query)
{
    memtable mt;

    seq_number seq = 100;
    string_view key = "abc";
    string_view val = "val";
    mt.add(key, val, 0);
    auto r = mt.query({key, seq});
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.seq(), 0); 
    EXPECT_EQ(r.value().key.type(), key_type::value); 
    EXPECT_EQ(r.value().value, val);
}

TEST(memtable, QueryMultiVersion)
{
    const auto iterations = 10;

    memtable mt;
    string_view key = "abc";

    for (int i = 0; i < iterations; ++i) {
        mt.add(key, std::to_string(i), i);
    }
    
    for (int i = 0; i < iterations; ++i) {
        const seq_number seq = i;
        auto r = mt.query({key, seq});
        EXPECT_TRUE(r);
        EXPECT_EQ(r.value().key.user_key(), key);
        EXPECT_EQ(r.value().key.seq(), i); 
        EXPECT_EQ(r.value().key.type(), key_type::value); 
        EXPECT_EQ(r.value().value, std::to_string(i));
    }

    // query latest
    auto r = mt.query({key, 100});
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.seq(), iterations - 1);
    EXPECT_EQ(r.value().key.type(), key_type::value); 
    EXPECT_EQ(r.value().value, std::to_string(iterations - 1)); 
}

TEST(memtable, Delete)
{
    memtable mt;

    seq_number seq = 1;
    string_view key = "abc";
    string_view val = "val";
    mt.add(key, val, seq);
    auto r = mt.query({key, seq});
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.user_key(), key);
    EXPECT_EQ(r.value().key.seq(), seq); 
    EXPECT_EQ(r.value().key.type(), key_type::value); 
    EXPECT_EQ(r.value().value, val);

    mt.remove(key, seq + 1);
    r = mt.query({key, seq + 2});
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value().key.type(), key_type::tombsome);
}

TEST(memtable, QueryRange)
{
    const auto N = 10;

    memtable mt;
    seq_number seq1 = 10;
    seq_number seq2 = 20;
    seq_number seq3 = 30;

    for (int i = 0; i < N; ++i) {
        mt.add(to_string(i), to_string(i), seq1);
    }

    auto r1 = mt.query_range({to_string(1), to_string(5)}, seq1);
    auto r2 = mt.query_range({to_string(1), to_string(5)}, seq2);

    EXPECT_EQ(r1.size(), 5);
    EXPECT_EQ(r2.size(), 5);
    EXPECT_TRUE(equal(r1.begin(), r1.end(), r2.begin(), r2.end(), [](const auto& a, const auto& b){
        return a.key == b.key && a.value == b.value;
    }));

    auto iter = r1.begin();
    for (int i = 1; i <= 5; ++i, ++iter) {
        EXPECT_NE(iter, r1.end());

        const auto kv = *iter;
        EXPECT_EQ(kv.key.user_key(), to_string(i));
        EXPECT_EQ(kv.key.seq(), seq1);
        EXPECT_EQ(kv.key.type(), key_type::value);
        EXPECT_EQ(kv.value, to_string(i));
    }

    for (int i = 0; i < N; i += 2) {
        mt.remove(to_string(i), seq2);
    }

    auto r3 = mt.query_range({to_string(0), to_string(N)}, seq3);
    int i = 0;
    for (const auto& kv: r3) {
        EXPECT_EQ(kv.key.user_key(), to_string(i));

        if (i % 2 == 0) {
            EXPECT_EQ(kv.key.seq(), seq2);
            EXPECT_EQ(kv.key.type(), key_type::tombsome);
        } else {
            EXPECT_EQ(kv.key.seq(), seq1);
            EXPECT_EQ(kv.key.type(), key_type::value);
            EXPECT_EQ(kv.value, to_string(i));
        }

        ++i;
    }
}