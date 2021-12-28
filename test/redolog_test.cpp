#include <gtest/gtest.h>
#include "memtable/redolog.h"
#include "write_batch_accessor.h"
#include "test_util.h"

using namespace cloudkv;

TEST(redolog, Write)
{
    auto p = fs::temp_directory_path() / "test";
    fs::remove(p);

    redolog redo { p };

    write_batch batch;
    batch.add("abc", "efg");
    redo.write(write_batch_accessor(batch).to_bytes());

    EXPECT_TRUE(fs::file_size(p) > 0);
}