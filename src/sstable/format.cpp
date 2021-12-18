#include "cloudkv/exception.h"
#include "sstable/format.h"
#include "util/fmt_std.h"

using namespace cloudkv;
using namespace cloudkv::sst;

block_handle::block_handle(std::string_view buf)
{
    if (buf.size() < block_handle_size) {
        throw data_corrupted{ "block handle too small" };
    }

    offset_ = DecodeFixed64(buf.data());
    length_ = DecodeFixed64(buf.data() + sizeof(offset_));
}

void block_handle::encode_to(std::string* out)
{
    PutFixed64(out, offset_);
    PutFixed64(out, length_);
}

footer::footer(std::string_view buf)
    : data_index_(-1, -1), meta_index_(-1, -1)
{
    assert(buf.size() == footer_size);

    data_index_ = block_handle(buf);
    buf.remove_prefix(block_handle::block_handle_size);

    meta_index_ = block_handle(buf);
    buf.remove_prefix(block_handle::block_handle_size);

    const auto magic = DecodeFixed64(buf.data());
    buf.remove_prefix(sizeof(magic));
    if (magic != table_magic) {
        throw data_corrupted{ fmt::format("footer corrupted, magic={}, expect={}", magic, table_magic) };
    }
}

void footer::encode_to(std::string* out)
{
    out->reserve(out->size() + footer_size);

    data_index_.encode_to(out);
    meta_index_.encode_to(out);

    PutFixed64(out, table_magic);
}