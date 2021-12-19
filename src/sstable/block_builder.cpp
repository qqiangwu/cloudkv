#include <cassert>
#include "sstable/block_builder.h"
#include "sstable/format.h"

using namespace cloudkv;

block_builder::block_builder(const options& opts)
{
    buf_.reserve(opts.block_size);
}

void block_builder::add(std::string_view key, std::string_view value)
{
    assert(!key.empty());

    const auto offset = buf_.size();

    encode_str(&buf_, key);
    encode_str(&buf_, value);

    offset_.push_back(offset);
}

std::string_view block_builder::done()
{
    for (const auto offset: offset_) {
        PutFixed32(&buf_, offset);
    }

    PutFixed32(&buf_, offset_.size());

    return buf_;
}