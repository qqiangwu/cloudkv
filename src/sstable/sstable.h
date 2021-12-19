#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <istream>
#include "core.h"
#include "cloudkv/iter.h"
#include "sstable/format.h"

namespace cloudkv {

class sstable {
public:
    explicit sstable(const path_t& file);

    iter_ptr iter();

    std::string_view min() const
    {
        return key_min_;
    }

    std::string_view max() const
    {
        return key_max_;
    }

    std::uint64_t count() const
    {
        return count_;
    }

    std::uint64_t size_in_bytes() const
    {
        return size_in_bytes_;
    }

    const path_t& path() const
    {
        return path_;
    }

private:
    void load_meta_(std::istream& in, sst::block_handle metahandle);

private:
    class sstable_iter;

private:
    path_t path_;

    sst::block_handle dataindex_;
    std::string key_min_;
    std::string key_max_;
    std::uint64_t count_ = 0;
    std::uint64_t size_in_bytes_ = 0;
};

using sstable_ptr = std::shared_ptr<sstable>;

}