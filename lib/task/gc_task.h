#pragma once

#include "task/default_task.h"
#include "path_conf.h"
#include "gc_root.h"

namespace cloudkv {

class gc_task : public default_task {
public:
    gc_task(const path_conf& db_path, gc_root& gcroot, std::uint64_t committed_file_id);

    void run() override;

    std::string_view name() const noexcept override
    {
        return "gc";
    }

private:
    const path_conf& db_path_;
    gc_root& gc_root_;
    const std::uint64_t committed_file_id_;
};

}