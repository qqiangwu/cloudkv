#pragma once

#include "task/default_task.h"
#include "path_conf.h"

namespace cloudkv {

class gc_task : public default_task {
public:
    gc_task(const path_conf& db_path, std::uint64_t committed_lsn);

    void run() override;

    std::string_view name() const noexcept override
    {
        return "gc";
    }

private:
    const path_conf& db_path_;
    const std::uint64_t committed_lsn_;
};

}