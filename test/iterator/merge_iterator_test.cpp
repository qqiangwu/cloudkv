#include <cstddef>
#include <gtest/gtest.h>
#include "memtable/memtable.h"
#include "iterator/merge_iterator.h"
#include "test_util.h"

using namespace std;
using namespace cloudkv;

TEST(merge_iterator, NonOverlap)
{
    auto kv = make_kv(1024);

    memtable mt1;
    memtable mt2;
    auto kviter = kv.begin();
    for (size_t i = 0; i < kv.size() / 2; ++i, ++kviter) {
        mt1.add(key_type::value, kviter->first, kviter->second);
    }
    for (size_t i = kv.size() / 2; i < kv.size(); ++i, ++kviter) {
        mt2.add(key_type::value, kviter->first, kviter->second);
    }

    std::vector<iter_ptr> iterators;
    iterators.push_back(mt1.iter());
    iterators.push_back(mt2.iter());
    auto iter = std::make_unique<merge_iterator>(std::move(iterators));

    auto kv_iter = kv.begin();

    for (iter->seek_first(); !iter->is_eof(); iter->next(), ++kv_iter) {
        ASSERT_TRUE(kv_iter != kv.end());

        const auto [k, v] = iter->current();
        const auto ikey = internal_key::parse(k);
        ASSERT_EQ(ikey.user_key(), kv_iter->first);
        ASSERT_TRUE(ikey.is_value());
        ASSERT_EQ(v, kv_iter->second);
    }
}

TEST(merge_iterator, Overlap)
{
    auto kv = make_kv(1024);

    memtable mt1;
    memtable mt2;
    int i = 0;
    for (auto& [k, v]: kv) {
        mt1.add(key_type::value, k, v);

        if (++i % 8 == 0) {
            v += ".updated";
            mt2.add(key_type::value, k, v);
        }
    }

    std::vector<iter_ptr> iterators;
    iterators.push_back(mt1.iter());
    iterators.push_back(mt2.iter());
    auto iter = std::make_unique<merge_iterator>(std::move(iterators));

    auto kv_iter = kv.begin();

    for (iter->seek_first(); !iter->is_eof(); iter->next(), ++kv_iter) {
        ASSERT_TRUE(kv_iter != kv.end());

        const auto [k, v] = iter->current();
        const auto ikey = internal_key::parse(k);
        ASSERT_EQ(ikey.user_key(), kv_iter->first);
        ASSERT_TRUE(ikey.is_value());
        ASSERT_EQ(v, kv_iter->second);
    }
}

TEST(merge_iterator, DeletedLater)
{
    auto kv = make_kv(1024);

    memtable mt1;
    memtable mt2;
    int i = 0;
    for (auto& [k, v]: kv) {
        mt1.add(key_type::value, k, v);

        if (++i % 8 == 0) {
            v.clear();
            mt2.add(key_type::tombsome, k, "");
        }
    }

    std::vector<iter_ptr> iterators;
    iterators.push_back(mt1.iter());
    iterators.push_back(mt2.iter());
    auto iter = std::make_unique<merge_iterator>(std::move(iterators));

    auto kv_iter = kv.begin();

    for (iter->seek_first(); !iter->is_eof(); iter->next(), ++kv_iter) {
        ASSERT_TRUE(kv_iter != kv.end());

        const auto [k, v] = iter->current();
        const auto ikey = internal_key::parse(k);
        ASSERT_EQ(ikey.user_key(), kv_iter->first);

        if (v.empty()) {
            ASSERT_TRUE(ikey.is_deleted());
        } else {
            ASSERT_TRUE(ikey.is_value());
            ASSERT_EQ(v, kv_iter->second);
        }
    }
}

TEST(merge_iterator, PutAfterDelete)
{
    auto kv = make_kv(1024);

    memtable mt1;
    memtable mt2;
    int i = 0;
    for (auto& [k, v]: kv) {
        mt1.add(key_type::tombsome, k, v);

        if (++i % 8 == 0) {
            v.clear();
            mt2.add(key_type::value, k, "");
        }
    }

    std::vector<iter_ptr> iterators;
    iterators.push_back(mt1.iter());
    iterators.push_back(mt2.iter());
    auto iter = std::make_unique<merge_iterator>(std::move(iterators));

    auto kv_iter = kv.begin();

    for (iter->seek_first(); !iter->is_eof(); iter->next(), ++kv_iter) {
        ASSERT_TRUE(kv_iter != kv.end());

        const auto [k, v] = iter->current();
        const auto ikey = internal_key::parse(k);
        ASSERT_EQ(ikey.user_key(), kv_iter->first);

        if (!v.empty()) {
            ASSERT_TRUE(ikey.is_deleted());
        } else {
            ASSERT_TRUE(ikey.is_value());
            ASSERT_EQ(v, kv_iter->second);
        }
    }
}