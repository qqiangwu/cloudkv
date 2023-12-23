#pragma once

#include <atomic>
#include "task/task.h"

namespace cloudkv {

class default_task : public task {
public:
    void cancel() override
    {
        cancelled_.store(true, std::memory_order_release);
    }

    bool is_cancelled() override
    {
        return cancelled_.load(std::memory_order_acquire);
    } 

private:
    std::atomic_bool cancelled_ = false;
};

using task_ptr = std::unique_ptr<task>;

}