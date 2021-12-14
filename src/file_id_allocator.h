#pragma once

#include <cstdint>
#include <atomic>
#include <cassert>

namespace cloudkv {

class file_id_allocator {
public:
    explicit file_id_allocator(std::uint64_t next = 1)
        : next_(next)
    {
        assert(next);
    }

    void reset(uint64_t next)
    {
        next_ = next;
    }

    std::uint64_t alloc()
    {
        assert(next_);
        return next_++;
    }

    std::uint64_t peek_next() const
    {
        return next_;
    }

private:
    std::atomic_uint64_t next_;
};

}