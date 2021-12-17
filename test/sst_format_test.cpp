#include <gtest/gtest.h>
#include "sstable/format.h"
#include "cloudkv/exception.h"

using namespace cloudkv;
using namespace cloudkv::sst;

namespace cloudkv::sst {

inline bool operator==(block_handle a, block_handle b)
{
    return a.offset() == b.offset()
        && a.length() == b.length();
}

}

TEST(sst_format, BlockHandle)
{
    const std::uint64_t offset = 32;
    const std::uint64_t length = 128;
    block_handle handle(32, 128);

    ASSERT_EQ(handle.offset(), offset);
    ASSERT_EQ(handle.length(), length);

    std::string buf;
    handle.encode_to(&buf);
    ASSERT_EQ(buf.size(), block_handle::block_handle_size);

    block_handle handle2(buf);
    ASSERT_EQ(handle2.offset(), offset);
    ASSERT_EQ(handle2.length(), length);
}

TEST(sst_format, Footer)
{
    block_handle dataindex(0, 128);
    block_handle metaindex(256, 1024 * 1024);

    footer foot(dataindex, metaindex);
    ASSERT_EQ(foot.data_index(), dataindex);
    ASSERT_EQ(foot.meta_index(), metaindex);

    std::string buf;
    foot.encode_to(&buf);
    ASSERT_EQ(buf.size(), footer::footer_size);

    footer foot2(buf);
    ASSERT_EQ(foot2.data_index(), dataindex);
    ASSERT_EQ(foot2.meta_index(), metaindex);

    ++buf.back();
    ASSERT_THROW(footer{buf}, data_corrupted);
}