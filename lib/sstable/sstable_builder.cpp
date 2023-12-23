#include <cassert>
#include <cstdint>
#include <fmt/core.h>
#include "util/coding.h"
#include "util/fmt_std.h"
#include "util/exception_util.h"
#include "sstable/format.h"
#include "sstable/sstable_builder.h"

using namespace std;
using namespace cloudkv;

sstable_builder::sstable_builder(const options& opts, std::ostream& out)
    : options_(opts),
      out_(out),
      datablock_(options_),
      datablock_index_(options_)
{
    if (!out_) {
        throw_system_error(fmt::format("create sst failed"));
    }

    out_.exceptions(std::ios::failbit | std::ios::badbit);
}

sstable_builder::sstable_builder(const options& opts, const path_t& p)
    : options_(opts),
      path_(p),
      buf_(std::make_unique<std::ofstream>(p, std::ios::binary)),
      out_(*buf_.get()),
      datablock_(options_),
      datablock_index_(options_)
{
    if (!out_) {
        throw_system_error(fmt::format("create sst {} failed", path_));
    }

    out_.exceptions(std::ios::failbit | std::ios::badbit);
}

void sstable_builder::add(std::string_view key, std::string_view value)
{
    assert(!key.empty());
    assert(first_key_.empty() || first_key_ <= last_key_);
    assert(last_key_.empty() || last_key_ < key);

    datablock_.add(key, value);
    if (first_key_.empty()) {
        first_key_ = key;
    }
    last_key_ = key;
    size_in_bytes_ += key.size() + value.size() + 2 * sizeof(std::uint32_t);  // uint32_t for str length
    ++entry_count_;

    if (datablock_.size_in_bytes() >= options_.block_size) {
        commit_datablock_();
    }
}

void sstable_builder::done()
{
    flush_pending_block_();
    flush_footer_();

    // commit
    out_.flush();
}

sst::block_handle sstable_builder::flush_block_(std::string_view content)
{
    assert(!content.empty());

    const std::uint64_t offset = out_.tellp();
    const std::uint64_t length = content.length();
    // todo: add crc

    out_.write(content.data(), content.size());

    return sst::block_handle{ offset, length };
}

sst::block_handle sstable_builder::flush_metablock_()
{
    block_builder metablock(options_);

    std::string buf;
    buf.reserve(sizeof(entry_count_));
    PutFixed64(&buf, entry_count_);

    metablock.add(sst::metablock_first_key, first_key_);
    metablock.add(sst::metablock_last_key, last_key_);
    metablock.add(sst::metablock_entry_count, buf);

    return flush_block_(metablock.done());
}

void sstable_builder::commit_datablock_()
{
    assert(datablock_.size_in_bytes() > 0);
    assert(!last_key_.empty());

    auto handle = flush_block_(datablock_.done());

    std::string buf;
    handle.encode_to(&buf);
    datablock_index_.add(last_key_, buf);

    datablock_.reset();
}

void sstable_builder::flush_pending_block_()
{
    if (datablock_.size_in_bytes() == 0) {
        return;
    }

    commit_datablock_();
}

void sstable_builder::flush_footer_()
{
    auto dataindex_handle = flush_block_(datablock_index_.done());
    auto meta_handle = flush_metablock_();

    sst::footer foot(dataindex_handle, meta_handle);

    std::string buf;
    foot.encode_to(&buf);

    out_.write(buf.data(), buf.size());
}