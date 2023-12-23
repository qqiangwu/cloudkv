#pragma once

#include <mutex>
#include <set>
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
        cloudkv::task* task;
        const error_handler handler;
    };

    void run_task_(const task_ctx& ctx) noexcept;

private:
    boost::executors::basic_thread_pool worker_;

    struct task_cmp : private std::less<task_ptr> {
        using is_transparent = void;

        using std::less<task_ptr>::operator();

        bool operator()(const task_ptr& x, task* y) const
        {
            return x.get() < y;
        }

        bool operator()(task* x, const task_ptr& y) const
        {
            return x < y.get();
        }
    };

    std::mutex mut_;
    std::set<task_ptr, task_cmp> tasks_;
};

}
