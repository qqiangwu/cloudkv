#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <fstream>
#include "core.h"
#include "kv_format.h"

namespace cloudkv {

class redolog_reader {
public:
    explicit redolog_reader(const path_t& p);

    // return nullopt if EOF
    // the return string_view keep valid until next call
    std::optional<std::string_view> next();

private:
    std::ifstream ifs_;
    std::string buf_;
};

}