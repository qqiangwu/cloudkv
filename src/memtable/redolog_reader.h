#pragma once

#include <string>
#include <optional>
#include <fstream>
#include "kv_format.h"

namespace cloudkv {

struct redo_entry {
    std::string key;
    std::string value;
    key_type op;
};

class redolog_reader {
public:
    explicit redolog_reader(const path_t& p);

    // return nullopt if EOF
    std::optional<redo_entry> next();

private:
    std::ifstream ifs_;
};

}