#include <filesystem>
#include <gtest/gtest.h>
#include "test_util.h"
#include "db_impl.h"

using namespace std;
using namespace cloudkv;

namespace fs = std::filesystem;

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

    for (int i = 0; i < key_count; ++i) {
        db->batch_add({
            { "key-" + std::to_string(i), "val-" + std::to_string(i) }
        });
    }

    for (int i = 0; i < key_count; ++i) {
        auto r = db->query("key-" + std::to_string(i));
        ASSERT_TRUE(r);
        ASSERT_EQ(r.value(), "val-" + std::to_string(i)); 
    }

    db.reset();
    db = open(db_root.native(), opts);

    for (int i = 0; i < key_count; ++i) {
        auto r = db->query("key-" + std::to_string(i));
        ASSERT_TRUE(r);
        ASSERT_EQ(r.value(), "val-" + std::to_string(i)); 
    }
}