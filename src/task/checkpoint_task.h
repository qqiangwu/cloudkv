#pragma once

#include <functional>
#include "memtable/memtable.h"
#include "sstable/sstable.h"
#include "task/default_task.h"
#include "path_conf.h"
#include "file_id_allocator.h"

namespace cloudkv {

class checkpoint_task : public default_task {
public:
    using callback_fn = std::function<void(sstable_ptr)>;

    checkpoint_task(const path_conf& db_path, file_id_allocator& id_alloc, memtable_ptr memtable, callback_fn fn)
        : db_path_(db_path),
          file_id_alloc_(id_alloc),
          memtable_(std::move(memtable)),
          notify_success_(std::move(fn))
    {
    }

    void run() override;

    std::string_view name() const noexcept override
    {
        return "checkpoint";
    }

private:
    const path_conf& db_path_;
    file_id_allocator& file_id_alloc_;
    const memtable_ptr memtable_;
    const callback_fn notify_success_;
};

}