#include <gtest/gtest.h>
#include "blockcache/cache.h"
#include "util/format.h"

using namespace cloudkv;
using namespace cloudkv::cache;

block mock_block(std::uint32_t size)
{
    std::string buf(size * sizeof(std::uint32_t) * 2, ' ');

    PutFixed32(&buf, size);

    return block(buf);
}

TEST(cache, Simple)
{
    simple_cache c;

    EXPECT_FALSE(c.get("1"));

    std::vector<cache_entry_ptr> entries;
    for (std::uint32_t s = 1; s < 8; ++s) {
        const auto key = std::to_string(s);

        auto entry1 = c.put(key, mock_block(s));
        EXPECT_TRUE(entry1);
        EXPECT_EQ(entry1->key(), key);

        entries.push_back(entry1);
    }

    for (std::uint32_t s = 1; s < 8; ++s) {
        const auto key = std::to_string(s);

        auto e = c.get(key);
        EXPECT_TRUE(e);
        EXPECT_EQ(e->key(), key);

        EXPECT_EQ(e, entries[s - 1]);
    }

    c.remove(entries.front()->key());

    EXPECT_FALSE(c.get(entries.front()->key()));
}