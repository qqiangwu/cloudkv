#pragma once

#include <vector>
#include <string>
#include "core.h"
#include "sstable/sstable.h"

namespace cloudkv {

struct metainfo {
    std::uint64_t committed_file_id = 0;
    std::uint64_t next_file_id = 1;
    std::vector<sstable_ptr> sstables;

    static metainfo load(const path_t& p);

    void store(const path_t& p);
};

};
