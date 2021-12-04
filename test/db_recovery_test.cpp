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
    opts.write_buffer_size = 1024;

    auto db = open(test_db, opts);
    ASSERT_TRUE(db);

    for (int i = 0; i < opts.write_buffer_size; ++i) {
        db->batch_add({
            { "key-" + std::to_string(i), "val-" + std::to_string(i) }
        });
    }

    for (int i = 0; i < opts.write_buffer_size; ++i) {
        auto r = db->query("key-" + std::to_string(i));
        EXPECT_TRUE(r);
        EXPECT_EQ(r.value(), "val-" + std::to_string(i)); 
    }

    db.reset();
    db = open(test_db, opts);

    for (int i = 0; i < opts.write_buffer_size; ++i) {
        auto r = db->query("key-" + std::to_string(i));
        EXPECT_TRUE(r);
        EXPECT_EQ(r.value(), "val-" + std::to_string(i)); 
    }
}