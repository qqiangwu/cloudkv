#include <filesystem>
#include <gtest/gtest.h>
#include <range/v3/view.hpp>
#include "test_util.h"
#include "db_impl.h"

using namespace std;
using namespace cloudkv;

namespace fs = std::filesystem;

using ranges::views::indices;

const string_view test_db = "test_db";

TEST(db_recover, Replay)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    options opts;
    opts.write_buffer_size = 512;
    const auto key_count = opts.write_buffer_size * 4;

    auto db = open(db_root.native(), opts);
    ASSERT_TRUE(db);

    for (const auto i: indices(key_count)) {
        db->batch_add({
            { "key-" + std::to_string(i), "val-" + std::to_string(i) }
        });
    }

    for (const auto i: indices(key_count)) {
        auto r = db->query("key-" + std::to_string(i));
        ASSERT_TRUE(r);
        ASSERT_EQ(r.value(), "val-" + std::to_string(i));
    }

    db.reset();
    db = open(db_root.native(), opts);

    for (const auto i: indices(key_count)) {
        auto r = db->query("key-" + std::to_string(i));
        ASSERT_TRUE(r);
        ASSERT_EQ(r.value(), "val-" + std::to_string(i));
    }
}