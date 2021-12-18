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

    std::string keybuf;
    for (const auto& kv: memtable_->items()) {
        kv.kv.key.encode_to(&keybuf);
        builder.add(keybuf, kv.kv.value);
        keybuf.clear();
    }

    builder.done();

    notify_success_(std::make_shared<sstable>(sst_path));
}