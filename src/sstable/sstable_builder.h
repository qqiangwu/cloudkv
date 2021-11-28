#pragma once

#include "sstable/sstable.h"

namespace cloudkv {

class sstable_builder {
public:
    explicit sstable_builder(const path_t file);

    void add(const internal_key& key, std::string_view value);
    
    void done();
};

}