#include <vector>
#include <range/v3/view.hpp>
#include <gtest/gtest.h>
#include "cloudkv/write_batch.h"

using namespace cloudkv;
using namespace ranges;

TEST(write_batch, Add)
{
    write_batch b;

    b.add("a", "a");
    EXPECT_EQ(b.size(), 1);

    for (const auto i: views::indices(1024)) {
        b.add(std::to_string(i), std::to_string(i));

        EXPECT_EQ(b.size(), 1 + i + 1);
    }
}

TEST(write_batch, Remove)
{
    write_batch b;

    b.remove("a");
    EXPECT_EQ(b.size(), 1);

    for (const auto i: views::indices(1024)) {
        b.remove(std::to_string(i));

        EXPECT_EQ(b.size(), 1 + i + 1);
    }
}

TEST(write_batch, Iterate)
{
    struct Entry {
        bool is_delete;
        std::string_view key;
        std::string_view val;
    };

    struct Collector : write_batch::handler {
        std::vector<Entry> entries;

        void add(std::string_view key, std::string_view val) override
        {
            entries.push_back({
                false, key, val
            });
        }

        void remove(std::string_view key) override
        {
            entries.push_back({ true, key, "" });
        }
    };

    std::vector<Entry> entries_expected = {
        { false, "1", "01" },
        { false, "2", "02" },
        { true, "3", "" },
        { false, "4", "04" }
    };

    write_batch batch;

    for (const auto& e: entries_expected) {
        if (e.is_delete) {
            batch.remove(e.key);
        } else {
            batch.add(e.key, e.val);
        }
    }

    Collector h;
    batch.iterate(&h);

    EXPECT_EQ(h.entries.size(), entries_expected.size());
    for (const auto i: views::indices(entries_expected.size())) {
        const auto& iterated = h.entries[i];
        const auto& expected = entries_expected[i];

        EXPECT_EQ(iterated.is_delete, expected.is_delete);
        EXPECT_EQ(iterated.key, expected.key);
        EXPECT_EQ(iterated.val, expected.val);
    }
}