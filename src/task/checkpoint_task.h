#pragma once

#include <functional>
#include "memtable/memtable.h"
#include "sstable/sstable.h"
#include "task/default_task.h"
#include "path_conf.h"
#include "file_id_allocator.h"
#include "gc_root.h"

namespace cloudkv {

class checkpoint_task : public default_task {
public:
    using callback_fn = std::function<void(sstable_ptr)>;

    struct checkpoint_ctx {
        const path_conf& db_path;
        file_id_allocator& file_id_alloc;
        gc_root& root;
        memtable_ptr memtable;
        callback_fn fn;
    };

    checkpoint_task(checkpoint_ctx ctx)
        : db_path_(ctx.db_path),
          file_id_alloc_(ctx.file_id_alloc),
          gc_root_(ctx.root),
          memtable_(std::move(ctx.memtable)),
          notify_success_(std::move(ctx.fn))
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
    gc_root& gc_root_;
    const memtable_ptr memtable_;
    const callback_fn notify_success_;
};

}