#pragma once

#include <boost/interprocess/sync/file_lock.hpp>
#include "core.h"

namespace cloudkv {

class file_lock : private noncopyable {
public:
    explicit file_lock(const path_t& p);

private:
    boost::interprocess::file_lock lock_;
};

}