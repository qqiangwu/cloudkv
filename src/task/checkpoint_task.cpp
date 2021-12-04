#include <chrono>
#include <charconv>
#include <fstream>
#include <spdlog/spdlog.h>
#include "task/checkpoint_task.h"
#include "sstable/sstable_builder.h"
#include "util/fmt_std.h"

using namespace cloudkv;

void checkpoint_task::run()
{
    using namespace fmt;
    using namespace std::chrono;

    const auto sst_path = db_path_.sst_path(steady_clock::now().time_since_epoch().count());
    
    std::ofstream ofs(sst_path, std::ios::binary);
    sstable_builder builder(ofs);

    for (const auto& [k, v]: memtable_->items()) {
        builder.add(k, v);
    }

    builder.done();

    notify_success_(std::make_shared<sstable>(sst_path));
}