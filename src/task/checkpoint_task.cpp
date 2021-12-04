#include <chrono>
#include <fstream>
#include <spdlog/spdlog.h>
#include "task/checkpoint_task.h"
#include "sstable/sstable_builder.h"
#include "util/fmt_std.h"
#include "cloudkv/exception.h"

using namespace cloudkv;

void checkpoint_task::run()
{
    using namespace fmt;
    using namespace std::chrono;

    const auto sst_path = db_path_.next_sst_path();
    
    std::ofstream ofs(sst_path, std::ios::binary);
    if (!ofs) {
        throw db_corrupted{ fmt::format("cannot write sst file {}", sst_path) };
    }
    sstable_builder builder(ofs);

    for (const auto& [k, v]: memtable_->items()) {
        builder.add(k, v);
    }

    builder.done();

    notify_success_(std::make_shared<sstable>(sst_path));
}