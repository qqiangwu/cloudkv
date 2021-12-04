#pragma once

#include <mutex>
#include <unordered_set>
#include <boost/noncopyable.hpp>
#include <boost/thread/executors/basic_thread_pool.hpp>
#include "task/task.h"

namespace cloudkv {

class task_manager : private boost::noncopyable {
public:
    task_manager();

    ~task_manager();

    // nothing happen if throws
    // return false if closed
    bool submit(task_ptr task);

private:
    void run_task_(const task_ptr& task) noexcept;

private:
    boost::executors::basic_thread_pool worker_;
    std::mutex mut_;
    std::unordered_set<task_ptr> tasks_;
};

}