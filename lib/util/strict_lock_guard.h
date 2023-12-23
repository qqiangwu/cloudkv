#pragma once

#include <cstdio>
#include <exception>
#include <boost/noncopyable.hpp>
#include "util/exception_util.h"

namespace cloudkv {

template <class Mutex>
class strict_lock_guard : private boost::noncopyable {
public:
    explicit strict_lock_guard(Mutex& m)
        : mut_(m)
    {
        try {
            mut_.lock();
        } catch (...) {
            std::fprintf(stderr, "lock mutex failed: %s", current_exception_msg());
            std::terminate();
        }
    }

    ~strict_lock_guard()
    {
        mut_.unlock();
    }

private:
    Mutex& mut_;
};

template <class Mutex>
strict_lock_guard(Mutex& m) -> strict_lock_guard<Mutex>;

}