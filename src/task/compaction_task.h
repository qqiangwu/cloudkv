#pragma once

#include <functional>
#include "sstable/sstable.h"
#include "task/default_task.h"
#include "path_conf.h"
#include "cloudkv/options.h"

namespace cloudkv {

class compaction_task : public default_task {
public:
    using sstable_vec = std::vector<sstable_ptr>;
    using callback_fn = std::function<void(const sstable_vec&, const sstable_vec&)>;

    compaction_task(const path_conf& db_path, const options& opts, const sstable_vec& sstables, callback_fn fn)
        : db_path_(db_path),
          opts_(opts),
          sstables_(sstables),
          notify_success_(std::move(fn))
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
    const sstable_vec sstables_;
    const callback_fn notify_success_;
};

}