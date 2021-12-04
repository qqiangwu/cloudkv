#include <spdlog/spdlog.h>
#include <scope_guard.hpp>
#include "task/task_manager.h"

using namespace cloudkv;

task_manager::task_manager()
    : worker_(2)
{
}

task_manager::~task_manager()
{
    worker_.close();

    for (const auto& task: tasks_) {
        task->cancel();
    }

    worker_.join();
    tasks_.clear();
}

bool task_manager::submit(task_ptr task)
{
    std::lock_guard _(mut_);

    auto iter = tasks_.insert(std::move(task)).first;
    SCOPE_FAIL{
        tasks_.erase(iter);
    };

    try {
        worker_.submit([&task = *iter, this]{
            run_task_(task);
        });

        return true;
    } catch (boost::sync_queue_is_closed&) {
        return false;
    }
}

void task_manager::run_task_(const task_ptr& task) noexcept
{
    try {
        task->run();
    } catch (std::exception& e) {
        spdlog::error("[task] run task {} failed: {}, ignored", task->name(), e.what());
    }

    // commit
    std::lock_guard _(mut_);
    tasks_.erase(task);
}