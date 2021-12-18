#pragma once

#include <cstdint>
#include <optional>
#include <memory>
#include <fstream>
#include "sstable/sstable.h"
#include "sstable/block_builder.h"
#include "sstable/format.h"

namespace cloudkv {

/**
 * @brief simple format: sorted string list
 *
 * [block1]
 * [block2]
 * [block3]
 * ...
 * [datablock index]
 * [metablock]
 * [footer]
 *      [datablock index handle]
 *      [metablockhandle]
 *      ...
 *
 * basic guarantee
 */
class sstable_builder {
public:
    // todo: remove this
    explicit sstable_builder(const options& opts, std::ostream& out);
    explicit sstable_builder(const options& opts, const path_t& p);

    void add(std::string_view key, std::string_view value);

    void done();

    std::uint64_t size_in_bytes() const
    {
        return size_in_bytes_;
    }

    const path_t& target() const
    {
        return path_;
    }

private:
    sst::block_handle flush_block_(std::string_view content);
    sst::block_handle flush_metablock_();

    void commit_datablock_();
    void flush_pending_block_();
    void flush_footer_();

private:
    const options& options_;
    const path_t path_;
    std::unique_ptr<std::ofstream> buf_;
    std::ostream& out_;

    block_builder datablock_;
    block_builder datablock_index_;

    std::uint64_t size_in_bytes_ = 0;
    std::uint64_t entry_count_ = 0;
    std::string first_key_;
    std::string last_key_;
};

}