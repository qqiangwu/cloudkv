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

bool task_manager::submit(task_ptr task, error_handler handler)
{
    std::lock_guard _(mut_);

    auto iter = tasks_.insert(std::move(task)).first;
    SCOPE_FAIL {
        tasks_.erase(iter);
    };

    try {
        worker_.submit([ctx = task_ctx{ *iter, std::move(handler) }, this]{
            run_task_(ctx);
        });

        return true;
    } catch (boost::sync_queue_is_closed&) {
        spdlog::warn("[task] task {} rejected because closed", task->name());
        return false;
    }
}

void task_manager::run_task_(const task_ctx& ctx) noexcept
{
    const auto& task = ctx.task;

    try {
        task->run();
    } catch (std::exception& e) {
        if (ctx.handler) {
            ctx.handler(e);
        }

        spdlog::error("[task] run task {} failed: {}, ignored", task->name(), e.what());
    }

    // commit
    std::lock_guard _(mut_);
    tasks_.erase(task);
}