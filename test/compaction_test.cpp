#include <map>
#include <gtest/gtest.h>
#include <range/v3/view.hpp>
#include <range/v3/action.hpp>
#include "task/compaction_task.h"
#include "sstable/sstable.h"
#include "sstable/sstable_builder.h"
#include "util/fmt_std.h"
#include "util/iter_util.h"
#include "test_util.h"
#include "file_id_allocator.h"

using namespace std;
using namespace cloudkv;

const string_view test_db = "test_db";

namespace {

file_id_allocator id_alloc;
gc_root gc;

}

TEST(compaction_task, Empty)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);
    fs::create_directories(db_path.sst_dir());

    std::vector<sstable_ptr> sstables;
    compaction_task({ db_path, options{}, id_alloc, gc, sstables, [](const auto& adds, const auto& dels){
        EXPECT_TRUE(adds.empty());
        EXPECT_TRUE(dels.empty());
    }}).run();
}

TEST(compaction_task, OneFile)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);
    fs::create_directories(db_path.sst_dir());

    std::vector<sstable_ptr> added;
    std::vector<sstable_ptr> removed;
    std::vector<sstable_ptr> sstables { make_sst_in_kv_format(db_path.sst_path(id_alloc.alloc())) };
    compaction_task({db_path, options{}, id_alloc, gc, sstables, [&](const auto& adds, const auto& dels){
        added = adds;
        removed = dels;
    }}).run();

    EXPECT_EQ(added.size(), 1);
    EXPECT_EQ(removed.size(), 1);

    const auto x = added.front();
    const auto y = removed.front();
    EXPECT_EQ(x->min(), y->min());
    EXPECT_EQ(x->max(), y->max());
    EXPECT_EQ(x->count(), y->count());

    auto vec1 = dump_all(x->iter());
    auto vec2 = dump_all(y->iter());
    EXPECT_EQ(vec1.size(), vec2.size());
    for (std::size_t i = 0; i < vec1.size(); ++i) {
        EXPECT_EQ(vec1[i].key.user_key(), vec2[i].key.user_key());
        EXPECT_EQ(vec1[i].value, vec2[i].value);
    }
}

TEST(compaction_task, NotOverlapFile)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);
    fs::create_directories(db_path.sst_dir());

    std::vector<sstable_ptr> added;
    std::vector<sstable_ptr> removed;
    std::vector<sstable_ptr> sstables {
        make_sst_in_kv_format(db_path.sst_path(id_alloc.alloc()), 0),
        make_sst_in_kv_format(db_path.sst_path(id_alloc.alloc()), 10),
        make_sst_in_kv_format(db_path.sst_path(id_alloc.alloc()), 20),
        make_sst_in_kv_format(db_path.sst_path(id_alloc.alloc()), 30)
    };
    compaction_task({db_path, options{}, id_alloc, gc, sstables, [&](const auto& adds, const auto& dels){
        added = adds;
        removed = dels;
    }}).run();

    EXPECT_EQ(added.size(), 1);
    EXPECT_EQ(removed.size(), 4);

    std::map<std::string, std::string> answer;
    for (const auto& sst: removed) {
        for (const auto& [key, val]: dump_all(sst->iter())) {
            answer.emplace(key.user_key(), val);
        }
    }

    const auto& sst = added.front();
    EXPECT_EQ(sst->count(), answer.size());

    auto iter = sst->iter();
    for (const auto& [k, v]: answer) {
        auto value_in_sst = query_key(iter, k);
        EXPECT_TRUE(value_in_sst);
        EXPECT_EQ(value_in_sst.value().value, v);
    }
}

TEST(compaction_task, OverlappedFile)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);
    fs::create_directories(db_path.sst_dir());

    const auto kv_count = 100;
    std::vector<sstable_ptr> added;
    std::vector<sstable_ptr> removed;
    std::vector<sstable_ptr> sstables {
        make_sst_in_kv_format(db_path.sst_path(0), 0, kv_count),
        make_sst_in_kv_format(db_path.sst_path(1), 50, kv_count),
        make_sst_in_kv_format(db_path.sst_path(2), 100, kv_count),
        make_sst_in_kv_format(db_path.sst_path(3), 150, kv_count),
        make_sst_in_kv_format(db_path.sst_path(4), 60, kv_count / 10)
    };
    file_id_allocator id_alloc{ 5 };
    compaction_task({db_path, options{}, id_alloc, gc, sstables, [&](const auto& adds, const auto& dels){
        added = adds;
        removed = dels;
    }}).run();

    ASSERT_EQ(added.size(), 1);
    ASSERT_EQ(removed.size(), sstables.size());

    std::map<std::string, std::string> answer;
    for (const auto& sst: removed) {
        for (const auto& [key, val]: dump_all(sst->iter())) {
            answer[std::string(key.user_key())] = val;
        }
    }

    const auto& sst = added.front();
    ASSERT_EQ(sst->count(), answer.size());

    auto iter = sst->iter();
    for (const auto& [k, v]: answer) {
        auto value_in_sst = query_key(iter, k);
        ASSERT_TRUE(value_in_sst);
        EXPECT_EQ(value_in_sst.value().value, v);
    }
}