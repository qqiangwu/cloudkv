#pragma once

#include <memory>
#include <string_view>
#include <boost/noncopyable.hpp>

namespace cloudkv {

class task : private boost::noncopyable {
public:
    virtual ~task() = default;

    virtual void run() = 0;
    virtual void cancel() = 0;
    virtual bool is_cancelled() = 0;

    virtual std::string_view name() const noexcept = 0;
};

using task_ptr = std::unique_ptr<task>;

}