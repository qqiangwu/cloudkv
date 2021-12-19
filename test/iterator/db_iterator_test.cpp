#include <gtest/gtest.h>
#include "memtable/memtable.h"
#include "iterator/db_iterator.h"
#include "test_util.h"

using namespace std;
using namespace cloudkv;

TEST(db_iterator, QueryRange)
{
    auto kv = make_kv(1024, 2);

    memtable mt;
    for (const auto& [k ,v]: kv) {
        mt.add(key_type::value, k, v);
    }

    auto iter = std::make_unique<db_iterator>(mt.iter());
    auto kv_iter = kv.begin();

    for (iter->seek_first(); !iter->is_eof(); iter->next(), ++kv_iter) {
        ASSERT_TRUE(kv_iter != kv.end());

        const auto [k, v] = iter->current();
        ASSERT_EQ(k, kv_iter->first);
        ASSERT_EQ(v, kv_iter->second);
    }
}