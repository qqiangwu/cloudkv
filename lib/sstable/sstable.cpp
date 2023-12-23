#include <string_view>
#include <fstream>
#include <filesystem>
#include <optional>
#include <fmt/core.h>
#include <range/v3/algorithm.hpp>
#include "cloudkv/exception.h"
#include "sstable/format.h"
#include "sstable/sstable.h"
#include "sstable/block.h"
#include "util/fmt_std.h"
#include "util/exception_util.h"

using namespace std;
using namespace cloudkv;
using namespace cloudkv::sst;

namespace fs = filesystem;

class sstable::sstable_iter : public kv_iter {
public:
    explicit sstable_iter(const path_t& path, block_handle dataindex);

    void seek_first() override;
    void seek(std::string_view key) override;

    bool is_eof() override;

    void next() override;

    key_value_pair current() override;

private:
    void load_block_();

private:
    std::ifstream in_;
    std::string buf_;

    std::unique_ptr<block> index_block_;
    iter_ptr index_iter_;

    std::unique_ptr<block> current_data_block_;
    iter_ptr current_data_iter_;
};

sstable::sstable_iter::sstable_iter(const path_t& path, sst::block_handle dataindex)
    : in_(path, std::ios::binary)
{
    if (!in_) {
        throw db_corrupted{ fmt::format("open sst {} failed", path) };
    }

    in_.exceptions(std::ios::failbit | std::ios::badbit);

    const std::uint64_t file_size = fs::file_size(path);
    if (file_size < dataindex.offset() + dataindex.length()) {
        throw data_corrupted{ fmt::format("invalid data index handle in sstable {}", path) };
    }

    std::string buf;
    buf.resize(dataindex.length());

    in_.seekg(dataindex.offset());
    in_.read(buf.data(), buf.size());
    index_block_ = std::make_unique<block>(buf);
    index_iter_ = index_block_->iter();
}

void sstable::sstable_iter::load_block_()
{
    assert(index_iter_);
    assert(!index_iter_->is_eof());

    auto content = index_iter_->current().value;
    sst::block_handle handle(content);

    buf_.resize(handle.length());

    in_.seekg(handle.offset());
    in_.read(buf_.data(), buf_.size());

    // fixme: empty block?
    current_data_block_ = std::make_unique<block>(buf_);
    current_data_iter_ = current_data_block_->iter();
}

void sstable::sstable_iter::seek_first()
{
    index_iter_->seek_first();
    if (index_iter_->is_eof()) {
        return;
    }

    load_block_();
    assert(current_data_iter_);

    current_data_iter_->seek_first();
}

void sstable::sstable_iter::seek(std::string_view key)
{
    index_iter_->seek(key);
    if (index_iter_->is_eof()) {
        return;
    }

    load_block_();

    current_data_iter_->seek(key);
}

bool sstable::sstable_iter::is_eof()
{
    return !current_data_iter_ || current_data_iter_->is_eof();
}

void sstable::sstable_iter::next()
{
    assert(!is_eof());
    assert(current_data_iter_);

    current_data_iter_->next();
    if (current_data_iter_->is_eof()) {
        current_data_iter_.reset();

        index_iter_->next();
        if (!index_iter_->is_eof()) {
            load_block_();
            current_data_iter_->seek_first();
        }
    }
}

kv_iter::key_value_pair sstable::sstable_iter::current()
{
    assert(!is_eof());
    return current_data_iter_->current();
}

sstable::sstable(const path_t& file)
    : path_(file)
{
    std::ifstream ifs(file, std::ios::binary);
    if (!ifs) {
        throw_system_error(fmt::format("open sst {} failed", file));
    }

    ifs.exceptions(std::ios::failbit | std::ios::badbit);

    const int64_t fsize = fs::file_size(file);
    if (fsize < footer::footer_size) {
        throw data_corrupted{ fmt::format("sst {} too small for a footer", file) };
    }

    std::string buf;
    buf.resize(footer::footer_size);
    ifs.seekg(fsize - footer::footer_size);
    ifs.read(buf.data(), buf.size());

    footer f(buf);
    dataindex_ = f.data_index();
    load_meta_(ifs, f.meta_index());

    size_in_bytes_ = fsize - footer::footer_size;
}

void sstable::load_meta_(std::istream& in, sst::block_handle metahandle)
{
    std::string buf;
    buf.resize(metahandle.length());

    in.seekg(metahandle.offset());
    in.read(buf.data(), buf.size());

    block blk(buf);

    auto it = blk.iter();
    bool count_seen = false;
    for (it->seek_first(); !it->is_eof(); it->next()) {
        const auto [k, v] = it->current();

        if (k == sst::metablock_first_key) {
            key_min_ = v;
        } else if (k == sst::metablock_last_key) {
            key_max_ = v;
        } else if (k == sst::metablock_entry_count) {
            if (v.size() != sizeof(std::uint64_t)) {
                throw data_corrupted{ fmt::format("invalid entry_count in metablock") };
            }

            count_seen = true;
            count_ = DecodeFixed64(v.data());
        }
    }

    if (!count_seen)  {
        throw data_corrupted{ "no entry_count in metablock" };
    }
    if (key_min_.empty() || key_max_.empty()) {
        throw data_corrupted{ fmt::format("invalid first_key or last_key in metablock") };
    }
}

iter_ptr sstable::iter()
{
    return std::make_unique<sstable_iter>(path_, dataindex_);
}
