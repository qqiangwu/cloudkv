#pragma once

#include <atomic>
#include <thread>
#include <mutex>
#include <future>
#include <memory>
#include <functional>
#include <vector>
#include <boost/noncopyable.hpp>
#include "cloudkv/write_batch.h"

namespace cloudkv {

class batch_executor : private boost::noncopyable {
public:
    using commit_fn = std::function<void(const write_batch&)>;

    explicit batch_executor(commit_fn fn);

    ~batch_executor();

    std::future<void> submit(const write_batch& batch);

private:
    void run_() noexcept;

    void wait_batch_();
    void swap_buffer_();
    void run_once_();

private:
    const commit_fn commit_fn_;

    struct task {
        const write_batch& batch;
        std::promise<void> promise;

        explicit task(const write_batch& b)
            : batch(b)
        {}
    };

    using task_ptr = std::unique_ptr<task>;

    std::mutex mut_;
    std::condition_variable work_doable_;
    std::vector<task_ptr> task_queue_;
    std::vector<task_ptr> working_set_;
    std::thread executor_;

    std::atomic_bool stopped_ { false };
};

}