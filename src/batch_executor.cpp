#include <vector>
#include <exception>
#include <chrono>
#include <spdlog/spdlog.h>
#include "batch_executor.h"

using namespace cloudkv;

namespace {

constexpr auto max_batch_size = 16;

}

batch_executor::batch_executor(commit_fn fn)
    : commit_fn_(fn),
      executor_([this]{ run_(); })
{}

batch_executor::~batch_executor()
{
    assert(task_queue_.empty());
    assert(working_set_.empty());
    assert(executor_.joinable());

    stopped_ = true;
    work_doable_.notify_all();
    executor_.join();
}

std::future<void> batch_executor::submit(const write_batch& batch)
{
    assert(!stopped_);
    assert(batch.size() > 0);

    auto t = std::make_unique<task>(batch);
    auto f = t->promise.get_future();
    bool need_notify = false;

    {
        std::lock_guard _(mut_);
        task_queue_.push_back(std::move(t));

        // commit
        need_notify = task_queue_.size() >= max_batch_size;
    }

    if (need_notify) {
        work_doable_.notify_one();
    }

    return f;
}

void batch_executor::run_() noexcept
{
    while (!stopped_) {
        try {
            wait_batch_();
            if (stopped_) {
                spdlog::warn("batch executor stopped");
                break;
            }

            swap_buffer_();
            run_once_();
        } catch (std::exception& e) {
            spdlog::error("commit failed: {}, ignored", e.what());

            auto ep = std::current_exception();

            for (const auto& p: working_set_) {
                p->promise.set_exception(ep);
            }
        }

        working_set_.clear();
    }

    assert(working_set_.empty());
}

void batch_executor::wait_batch_()
{
    using namespace std::chrono_literals;

    std::unique_lock l(mut_);
    while (!stopped_) {
        work_doable_.wait_for(l, 50us);

        if (!task_queue_.empty())  {
            break;
        }
    }
}

void batch_executor::swap_buffer_()
{
    std::lock_guard _(mut_);
    assert(!task_queue_.empty());
    assert(working_set_.empty());

    using namespace std;
    swap(working_set_, task_queue_);
}

void batch_executor::run_once_()
{
    write_batch batch;
    for (const auto& p: working_set_) {
        batch.add(p->batch);
    }

    if (batch.size() > 0) {
        commit_fn_(batch);
    }

    // commit
    for (const auto& p: working_set_) {
        p->promise.set_value();
    }
}