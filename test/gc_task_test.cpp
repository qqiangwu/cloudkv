#include <filesystem>
#include <gtest/gtest.h>
#include "task/gc_task.h"
#include "test_util.h"

using namespace std;
using namespace cloudkv;

namespace fs = std::filesystem;

namespace {

const string_view test_db = "test_db";
gc_root gc;

}

TEST(gc_task, Empty)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);

    gc_task task(db_path, gc, 0);
    try {
        task.run();
        ASSERT_TRUE(!"should throws");
    } catch (std::system_error& e) {
        EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
    }
}

TEST(gc_task, Run)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);
    fs::create_directory(db_path.root());
    fs::create_directory(db_path.redo_dir());
    fs::create_directory(db_path.sst_dir());

    const auto total_files = 32;
    const auto committed_file_id = 16;
    for (auto i = 0; i < total_files; ++i) {
        create_file(db_path.redo_path(i));
    }

    for (auto i = 0; i < total_files; ++i) {
        ASSERT_TRUE(fs::exists(db_path.redo_path(i)));
    }

    gc_task(db_path, gc, committed_file_id).run();

    for (auto i = 0; i < total_files; ++i) {
        if (i <= committed_file_id) {
            ASSERT_FALSE(fs::exists(db_path.redo_path(i)));
        } else {
            ASSERT_TRUE(fs::exists(db_path.redo_path(i)));
        }
    }
}

TEST(gc_task, IgnoreUnknownFiles)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);

    fs::create_directory(db_path.root());
    fs::create_directory(db_path.redo_dir());
    fs::create_directory(db_path.sst_dir());

    const auto unknown_dir = db_path.redo_dir() / "unknown-dir";
    const auto unknown_file = db_path.redo_dir() / "unknown-file";
    fs::create_directory(unknown_dir);
    create_file(unknown_file);

    ASSERT_TRUE(fs::exists(unknown_dir));
    ASSERT_TRUE(fs::exists(unknown_file));

    gc_task(db_path, gc, std::uint64_t(-1)).run();

    ASSERT_TRUE(fs::exists(unknown_dir));
    ASSERT_TRUE(fs::exists(unknown_file));
}

TEST(gc_task, SstableGC)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);

    fs::create_directory(db_path.root());
    fs::create_directory(db_path.redo_dir());
    fs::create_directory(db_path.sst_dir());

    std::vector<path_t> reachable;
    std::vector<path_t> nonreachable;
    gc_root gc;
    for (int i = 0; i < 10; ++i) {
        auto p = db_path.sst_path(i);
        create_file(p);

        if (i % 2 == 0) {
            reachable.push_back(p);
            gc.add_temporary(p);
        } else {
            nonreachable.push_back(p);
        }
    }

    gc_task(db_path, gc, 0).run();

    for (const auto& p: reachable) {
        ASSERT_TRUE(fs::exists(p));
    }

    for (const auto& p: nonreachable) {
        ASSERT_FALSE(fs::exists(p));
    }
}