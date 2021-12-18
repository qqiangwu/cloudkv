#include <map>
#include <string>
#include <gtest/gtest.h>
#include "sstable/block.h"
#include "sstable/block_builder.h"
#include "cloudkv/options.h"
#include "test_util.h"

using namespace cloudkv;

namespace {

block make_block(const std::map<std::string, std::string>& kv)
{
    block_builder builder({});

    for (const auto& [k, v]: kv) {
        builder.add(k, v);
    }

    return block(builder.done());
}

}

TEST(block, Builder)
{
    std::map<std::string, std::string> kv = make_kv(8);
    block_builder builder({});

    for (int i = 0; i < 2; ++i) {
        ASSERT_EQ(builder.size_in_bytes(), 0);

        for (const auto& [k, v]: kv) {
            builder.add(k, v);
        }

        block blk(builder.done());
        ASSERT_EQ(blk.count(), kv.size());

        auto iter = blk.iter();
        auto iter2 = kv.begin();
        for (iter->seek_first(); !iter->is_eof(); iter->next(), ++iter2) {
            const auto kv_in_block = iter->current();

            ASSERT_EQ(kv_in_block.key, iter2->first);
            ASSERT_EQ(kv_in_block.value, iter2->second);
        }

        ASSERT_EQ(iter2, kv.end());

        builder.reset();
    }
}

TEST(block, Seek)
{
    std::map<std::string, std::string> kv;

    kv.emplace("0", "0");
    kv.emplace("2", "2");
    kv.emplace("3", "3");
    kv.emplace("5", "5");
    kv.emplace("7", "7");

    auto blk = make_block(kv);
    ASSERT_EQ(blk.count(), kv.size());

    auto it = blk.iter();
    ASSERT_TRUE(it);

    it->seek_first();
    auto value = it->current();
    ASSERT_EQ(value.key, kv.begin()->first);
    ASSERT_EQ(value.value, kv.begin()->second);

    for (const auto& [k, v]: kv) {
        it->seek(k);
        ASSERT_TRUE(!it->is_eof());

        auto value = it->current();
        ASSERT_EQ(value.key, k);
        ASSERT_EQ(value.value, v);
    }

    it->seek("4");
    ASSERT_TRUE(!it->is_eof());

    value = it->current();
    ASSERT_EQ(value.key, "5");
    ASSERT_EQ(value.value, "5");

    it->seek("8");
    ASSERT_TRUE(it->is_eof());
}