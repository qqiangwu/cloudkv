#pragma once

#include <vector>
#include "core.h"
#include "sstable/sstable.h"

namespace cloudkv {

struct meta {
    seq_number next_seq_;
    std::vector<sstable_ptr> sstables_;
};

};