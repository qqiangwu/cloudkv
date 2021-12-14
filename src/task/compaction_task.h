#pragma once

#include <functional>
#include "sstable/sstable.h"
#include "task/default_task.h"
#include "path_conf.h"
#include "cloudkv/options.h"
#include "file_id_allocator.h"
#include "gc_root.h"

namespace cloudkv {

class compaction_task : public default_task {
public:
    using sstable_vec = std::vector<sstable_ptr>;
    using callback_fn = std::function<void(const sstable_vec&, const sstable_vec&)>;

    struct compaction_ctx {
        const path_conf& db_path;
        const options& opts;
        file_id_allocator& file_id_alloc;
        gc_root& gcroot;
        sstable_vec sstables;
        callback_fn fn;
    };

    explicit compaction_task(compaction_ctx ctx)
        : db_path_(ctx.db_path),
          opts_(ctx.opts),
          file_id_alloc_(ctx.file_id_alloc),
          gc_root_(ctx.gcroot),
          sstables_(std::move(ctx.sstables)),
          notify_success_(std::move(ctx.fn))
    {
    }

    void run() override;

    std::string_view name() const noexcept override
    {
        return "compaction";
    }

private:
    sstable_vec choose_files_();

private:
    const path_conf& db_path_;
    const options& opts_;
    file_id_allocator& file_id_alloc_;
    gc_root& gc_root_;
    const sstable_vec sstables_;
    const callback_fn notify_success_;
};

}