#include <cstddef>
#include <cassert>
#include <vector>
#include <fstream>
#include <range/v3/view.hpp>
#include <spdlog/spdlog.h>
#include "task/compaction_task.h"
#include "sstable/sstable.h"
#include "sstable/sstable_builder.h"
#include "iterator/merge_iterator.h"
#include "cloudkv/exception.h"
#include "kv_format.h"

using namespace cloudkv;
using namespace ranges;

void compaction_task::run()
{
    auto sstables_to_compaction = choose_files_();
    if (sstables_to_compaction.empty()) {
        spdlog::info("[compaction] no sstables choosed for compaction");
        notify_success_({}, {});
        return;
    }

    spdlog::info("[compaction] started, {} sst", sstables_to_compaction.size());
    sstable_vec new_sstables;
    auto iter = std::make_unique<merge_iterator>(sstables_to_compaction | views::transform([](const auto& sst){
        return sst->iter();
    }) | to<std::vector>);

    temporary_gc_root gc { gc_root_ };
    std::optional<sstable_builder> builder;

    for (iter->seek_first(); !iter->is_eof() && !is_cancelled(); iter->next()) {
        if (!builder) {
            auto filepath = db_path_.sst_path(file_id_alloc_.alloc());
            gc.add(filepath);
            builder.emplace(opts_, filepath);
        }

        const auto current = iter->current();
        builder->add(current.key, current.value);
        if (builder->size_in_bytes() > opts_.sstable_size) {
            builder->done();

            new_sstables.push_back(std::make_shared<sstable>(builder->target()));

            builder.reset();
        }
    }

    if (is_cancelled()) {
        spdlog::warn("[compaction] task cancelled");
        notify_success_({}, {});
        return;
    }

    if (builder && builder->size_in_bytes() > 0) {
        builder->done();
        new_sstables.push_back(std::make_shared<sstable>(builder->target()));
    }

    spdlog::info("[compaction] completed, {} sst generated", new_sstables.size());
    notify_success_(new_sstables, sstables_to_compaction);
}

compaction_task::sstable_vec compaction_task::choose_files_()
{
    sstable_vec compaction_vec;

    // todo: refine this, now compaction all
    return sstables_;
}