#pragma once

#include <mutex>
#include <unordered_set>
#include <functional>
#include <boost/noncopyable.hpp>
#include <boost/thread/executors/basic_thread_pool.hpp>
#include "task/task.h"

namespace cloudkv {

class task_manager : private boost::noncopyable {
public:
    using error_handler = std::function<void(const std::exception&) /* noexcept */>;

    task_manager();

    ~task_manager();

    // nothing happen if throws
    // return false if closed
    bool submit(task_ptr task, error_handler handler = nullptr);

private:
    struct task_ctx {
        const task_ptr& task;
        const error_handler handler;
    };

    void run_task_(const task_ctx& ctx) noexcept;

private:
    boost::executors::basic_thread_pool worker_;
    std::mutex mut_;
    std::unordered_set<task_ptr> tasks_;
};

}