#include <chrono>
#include <fstream>
#include "task/checkpoint_task.h"
#include "sstable/sstable_builder.h"
#include "util/fmt_std.h"
#include "cloudkv/exception.h"

using namespace cloudkv;

void checkpoint_task::run()
{
    using namespace fmt;
    using namespace std::chrono;

    temporary_gc_root gc { gc_root_ };

    const auto sst_path = db_path_.sst_path(file_id_alloc_.alloc());
    gc.add(sst_path);

    std::ofstream ofs(sst_path, std::ios::binary);
    if (!ofs) {
        throw db_corrupted{ fmt::format("cannot write sst file {}", sst_path) };
    }

    // fixme
    sstable_builder builder({}, ofs);

    auto it = memtable_->iter();
    for (it->seek_first(); !it->is_eof(); it->next()) {
        const auto kv = it->current();
        builder.add(kv.key, kv.value);
    }

    builder.done();

    notify_success_(std::make_shared<sstable>(sst_path));
}