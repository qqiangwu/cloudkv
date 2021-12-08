#include <filesystem>
#include <gtest/gtest.h>
#include "test_util.h"
#include "replayer.h"
#include "util/fmt_std.h"
#include "memtable/redolog.h"
#include "cloudkv/exception.h"

using namespace std;
using namespace cloudkv;

namespace fs = std::filesystem;

const string_view test_db = "test_db";

TEST(replayer, EmptyRedo)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);
    fs::create_directories(db_path.redo_dir());
    create_file(db_path.redo_path(10));
    create_file(db_path.redo_path(20));
    create_file(db_path.redo_path(30));

    auto res = replayer(db_path, 5).replay();
    ASSERT_EQ(res.replayed_lsn, 5);
    ASSERT_TRUE(res.sstables.empty());
}

TEST(replayer, BadRedo)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);
    fs::create_directories(db_path.redo_dir());
    fs::create_directories(db_path.sst_dir());
    create_file(db_path.redo_path(10), "1234567");

    auto res = replayer(db_path, 5).replay();
    ASSERT_EQ(res.replayed_lsn, 5);
    ASSERT_TRUE(res.sstables.empty());
}

TEST(replayer, Replay)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);
    fs::create_directories(db_path.redo_dir());
    fs::create_directories(db_path.sst_dir());

    const auto max_lsn = 10;
    const auto persisted_lsn = 5;
    for (int i = 1; i <= max_lsn; ++i) {
        redolog log(db_path.redo_path(i));

        for (int k = 0; k < 100; ++k) {
            write_batch batch;

            batch.add(fmt::format("key-{}-{}", i, k), fmt::format("val-{}-{}", i, k));
            log.write(batch);
        }
    }

    auto res = replayer(db_path, persisted_lsn).replay();
    ASSERT_EQ(res.replayed_lsn, max_lsn);
    ASSERT_EQ(res.sstables.size(), max_lsn - persisted_lsn);
}