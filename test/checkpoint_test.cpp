#include <map>
#include <gtest/gtest.h>
#include <range/v3/view.hpp>
#include <range/v3/action.hpp>
#include "task/checkpoint_task.h"
#include "memtable/memtable.h"
#include "sstable/sstable.h"
#include "util/fmt_std.h"
#include "util/iter_util.h"
#include "test_util.h"
#include "file_id_allocator.h"
#include "gc_root.h"

using namespace std;
using namespace cloudkv;

const string_view test_db = "test_db";

namespace {

file_id_allocator id_alloc;
gc_root gc;

}

TEST(checkpoint_task, OneFile)
{
    auto db_root = fs::temp_directory_path() / test_db;
    DBCleaner _(db_root);

    path_conf db_path(db_root);
    fs::create_directories(db_path.sst_dir());

    auto kv = make_kv(1024);

    auto mt = std::make_shared<memtable>();
    for (const auto& [k ,v]: kv) {
        mt->add(key_type::value, k, v);
    }

    sstable_ptr sst;
    checkpoint_task({db_path, id_alloc, gc, mt, [&sst](sstable_ptr s){
        sst = std::move(s);
    }}).run();

    ASSERT_EQ(sst->count(), kv.size());

    auto kv_it = kv.begin();
    auto it = sst->iter();
    for (it->seek_first(); !it->is_eof(); it->next(), ++kv_it) {
        ASSERT_TRUE(kv_it != kv.end());

        const auto [key, val] = it->current();
        const auto ikey = internal_key::parse(key);
        ASSERT_EQ(ikey.user_key(), kv_it->first);
        ASSERT_EQ(val, kv_it->second);
        ASSERT_TRUE(ikey.is_value());
    }

    ASSERT_TRUE(kv_it == kv.end());
}