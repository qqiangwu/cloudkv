#pragma once

#include <cstdint>

namespace cloudkv {

struct options {
    bool open_only = false;
    std::uint64_t write_buffer_size = 4 * 1024 * 1024;
    std::uint64_t sstable_size = 16 * 1024 * 1024;
    std::uint64_t compaction_water_mark = 8;
};

}