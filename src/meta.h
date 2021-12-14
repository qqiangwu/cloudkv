#pragma once

#include <vector>
#include <string>
#include "core.h"
#include "sstable/sstable.h"

namespace cloudkv {

namespace detail {



}

struct metainfo {
    std::uint64_t committed_lsn = 0;
    std::uint64_t next_lsn = 1;
    std::vector<sstable_ptr> sstables;

    static metainfo load(const path_t& p);

    void store(const path_t& p);
};

};
