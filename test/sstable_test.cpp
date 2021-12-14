#include <map>
#include <gtest/gtest.h>
#include "sstable/sstable.h"
#include "test_util.h"

using namespace std;
using namespace ranges;
using namespace cloudkv;

TEST(sstable, BasicAttr)
{
    ScopedTmpDir dir { __func__ };

    map<string, string> kv;
    auto key_cnt = 1024;
    for (const auto i: views::ints(0, key_cnt)) {
        kv.emplace( __func__ + std::to_string(i), "value-" + std::to_string(i));
    }

    auto p = dir.path / "1";
    auto sst = make_sst(p, kv);

    ASSERT_EQ(sst->count(), kv.size());
    ASSERT_EQ(sst->path(), p);
    ASSERT_EQ(sst->min(), kv.begin()->first);
    ASSERT_EQ(sst->max(), kv.rbegin()->first);
}

TEST(sstable, Query)
{
    ScopedTmpDir dir { __func__ };

    map<string, string> kv;
    auto key_cnt = 1024;
    for (const auto i: views::ints(0, key_cnt)) {
        kv.emplace( __func__ + std::to_string(i), "value-" + std::to_string(i));
    }

    auto p = dir.path / "1";
    auto sst = make_sst(p, kv);

    for (const auto& [k, v]: kv) {
        auto r = sst->query(k);
        ASSERT_TRUE(r);
        ASSERT_EQ(r->key.user_key(), k);
        ASSERT_EQ(r->key.type(), key_type::value);
        ASSERT_EQ(r->value, v);

        r = sst->query("x" + k);
        ASSERT_FALSE(r);
    }
}

TEST(sstable, QueryRange)
{
    ScopedTmpDir dir { __func__ };

    map<string, string> kv;
    auto key_cnt = 1024;
    for (const auto i: views::ints(0, key_cnt)) {
        kv.emplace( __func__ + std::to_string(i), "value-" + std::to_string(i));
    }

    auto p = dir.path / "1";
    auto sst = make_sst(p, kv);

    ASSERT_EQ(sst->count(), kv.size());
    auto it1 = sst->iter();
    auto it2 = kv.begin();
    while (!it1->is_eof()) {
        ASSERT_NE(it2, kv.end());

        auto v1 = it1->next();
        ASSERT_EQ(v1.key.user_key(), it2->first);
        ASSERT_EQ(v1.value, it2->second);

        ++it2;
    }

    ASSERT_EQ(it2, kv.end());
}