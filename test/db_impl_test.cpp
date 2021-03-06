#include <string_view>
#include <filesystem>
#include <stdexcept>
#include <range/v3/view.hpp>
#include <range/v3/action.hpp>
#include <range/v3/algorithm.hpp>
#include <gtest/gtest.h>
#include "cloudkv/exception.h"
#include "db_impl.h"
#include "test_util.h"

using namespace std;
using namespace cloudkv;
using namespace ranges;

namespace fs = std::filesystem;

using views::indices;

constexpr string_view db_name = "test";

void remove_db()
{
    filesystem::remove_all(db_name);
}

struct Cleanup {
    Cleanup()
    {
        remove_db();
    }
};

TEST(db, OpenOnly)
{
    Cleanup _;

    options opts;
    opts.open_only = true;
    EXPECT_THROW(open(db_name, opts), std::invalid_argument);
}

TEST(db, BadDirStructure)
{
    Cleanup _;

    const path_t db_path = db_name;
    options opts;
    EXPECT_TRUE(open(db_name, opts));

    fs::remove_all(db_path / "meta");
    EXPECT_THROW(open(db_name, opts), db_corrupted);
}

TEST(db, Basic)
{
    Cleanup _;

    auto db = open(db_name, options());
    EXPECT_TRUE(db);

    const string_view key = "123";
    const string_view val = "780";
    const string_view bad_key = "12";

    EXPECT_FALSE(db->query(key));
    EXPECT_FALSE(db->query(bad_key));

    db->add(key, val);

    const auto r = db->query(key);
    EXPECT_TRUE(r);
    EXPECT_EQ(r.value(), val);

    EXPECT_FALSE(db->query(bad_key));
}

TEST(db, MultiInsert)
{
    Cleanup _;

    auto db = open(db_name, options());
    ASSERT_TRUE(db);

    auto raw_keys = views::ints(0, 1000);
    auto keys = raw_keys
        | views::transform([](int i){ return std::to_string(i); })
        | to<std::vector>()
        | actions::sort;
    auto key_values = keys
        | views::transform([](const auto& k){
            return std::make_tuple(k, "val-" + k);
        })
        | to<std::vector>();

    write_batch batch;
    for (const auto& [k, v]: key_values) {
        batch.add(k, v);
    }

    db->batch_add(batch);

    for (const auto& [key, val]: key_values) {
        const auto r = db->query(key);
        ASSERT_TRUE(r);
        ASSERT_EQ(r.value(), val);
    }
}

TEST(db, QueryRange)
{
    Cleanup _;

    auto db = open(db_name, options());
    ASSERT_TRUE(db);

    std::map<std::string, std::string> testkv;
    int i = 0;
    for (const auto& [k, v]: make_kv(2048, 3)) {
        if (++i % 8 == 0) {
            db->remove(k);
        } else {
            db->add(k, v);
            testkv.emplace(k, v);
        }
    }

    auto it = db->iter();
    auto test_it = testkv.begin();
    for (it->seek_first(); !it->is_eof(); it->next(), ++test_it) {
        ASSERT_TRUE(test_it != testkv.end());

        const auto& [testk, testv] = *test_it;
        const auto [dbkey, dbval] = it->current();

        ASSERT_EQ(testk, dbkey);
        ASSERT_EQ(testv, dbval);
    }

    ASSERT_TRUE(test_it == testkv.end());
}

TEST(db, Checkpoint)
{
    Cleanup _;

    options opts;
    opts.write_buffer_size = 1024;
    auto db = open(db_name, opts);

    for (auto i: indices(opts.write_buffer_size)) {
        db->add("key-" + std::to_string(i), "val-" + std::to_string(i));
    }

    for (auto i: indices(opts.write_buffer_size)) {
        auto r = db->query("key-" + std::to_string(i));
        EXPECT_TRUE(r);
        EXPECT_EQ(r.value(), "val-" + std::to_string(i));
    }
}

TEST(db, Persistent)
{
    Cleanup _;

    options opts;
    opts.write_buffer_size = 1024;
    auto db = open(db_name, opts);

    for (auto i: indices(opts.write_buffer_size)) {
        db->add("key-" + std::to_string(i), "val-" + std::to_string(i));
    }

    for (auto i: indices(opts.write_buffer_size)) {
        auto r = db->query("key-" + std::to_string(i));
        ASSERT_TRUE(r);
        ASSERT_EQ(r.value(), "val-" + std::to_string(i));
    }

    static_cast<db_impl*>(db.get())->TEST_flush();

    db.reset();
    db = open(db_name, opts);

    for (auto i: indices(opts.write_buffer_size)) {
        auto r = db->query("key-" + std::to_string(i));
        ASSERT_TRUE(r);
        ASSERT_EQ(r.value(), "val-" + std::to_string(i));
    }
}
