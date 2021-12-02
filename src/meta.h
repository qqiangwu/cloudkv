#pragma once

#include <vector>
#include "core.h"
#include "sstable/sstable.h"

namespace cloudkv {

struct meta {
    std::vector<sstable_ptr> sstables_;
};

};