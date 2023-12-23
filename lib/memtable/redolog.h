#pragma once

#include <fstream>
#include <memory>
#include <string_view>
#include "core.h"

namespace cloudkv {

class redolog : noncopyable {
public:
    explicit redolog(const path_t& p, std::uint64_t buffer_size = 4096);

    void write(std::string_view record);

private:
    std::ofstream out_;
    std::string buf_;
};

using redolog_ptr = std::shared_ptr<redolog>;

}