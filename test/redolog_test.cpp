#include <gtest/gtest.h>
#include "memtable/redolog.h"
#include "test_util.h"

using namespace cloudkv;

TEST(redolog, Write)
{
    auto p = fs::temp_directory_path() / "test";
    fs::remove(p);

    redolog redo { p };

    write_batch batch;
    batch.add("abc", "efg");
    redo.write(batch);

    EXPECT_TRUE(fs::file_size(p) > 0);
}