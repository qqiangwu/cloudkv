#include <vector>
#include <filesystem>
#include <gtest/gtest.h>
#include "gc_root.h"
#include "test_util.h"

using namespace cloudkv;

namespace fs = std::filesystem;

TEST(gc_root, TemporaryGCRoot)
{
    gc_root root;

    std::vector<path_t> files;
    files.push_back(fs::temp_directory_path() / "1");
    files.push_back(fs::temp_directory_path() / "2");
    files.push_back(fs::temp_directory_path() / "3");

    for (const auto& p: files) {
        ASSERT_FALSE(root.is_reachable(p));

        {
            temporary_gc_root subroot { root };
            subroot.add(p);

            ASSERT_TRUE(root.is_reachable(p));
        }

        ASSERT_FALSE(root.is_reachable(p));
    }

    {
        temporary_gc_root subroot { root };

        for (const auto& p: files) {
            subroot.add(p);
        }

        for (const auto& p: files) {
            ASSERT_TRUE(root.is_reachable(p));
        }
    }

    for (const auto& p: files) {
        ASSERT_FALSE(root.is_reachable(p));
    }
}

TEST(gc_root, SstableExpiration)
{
    auto base = fs::temp_directory_path() / "gc_root";
    DBCleaner _(base);

    fs::create_directory(base);

    std::vector<sstable_ptr> sstables_reachable;
    std::vector<sstable_ptr> sstables_nonreachable;
    gc_root gc;

    for (int i = 0; i < 10; ++i) {
        auto sst = make_sst(base / fmt::format("sst.{}", i), i);
        if (i % 2 == 0) {
            sstables_reachable.push_back(sst);
        } else {
            sstables_nonreachable.push_back(sst);
        }

        gc.add(sst);
    }

    using namespace ranges;
    for (const auto& sst: views::concat(sstables_reachable, sstables_nonreachable)) {
        ASSERT_TRUE(gc.is_reachable(sst->path()));
    }

    auto nonreachable_paths = sstables_nonreachable | views::transform([](const auto& sst){
        return sst->path();
    })
    | to<std::vector>();

    sstables_nonreachable.clear();

    for (const auto& sst: sstables_reachable) {
        ASSERT_TRUE(gc.is_reachable(sst->path()));
    }

    for (const auto& p: nonreachable_paths) {
        ASSERT_FALSE(gc.is_reachable(p));
    }
}