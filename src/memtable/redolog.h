#pragma once

#include <fstream>
#include <string>
#include <memory>
#include "core.h"
#include "memtable/write_batch.h"

namespace cloudkv {

class redolog : noncopyable {
public:
    explicit redolog(const path_t& p, std::uint64_t buffer_size = 4096);

    void write(const write_batch& batch);

    void flush();

private:
    std::ofstream out_;
    std::string buf_;
};

using redolog_ptr = std::shared_ptr<redolog>;

}