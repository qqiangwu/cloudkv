#pragma once

#include <cstdint>
#include "iter.h"

namespace cloudkv {

class block {
public:
    explicit block(std::string_view buf);

    iter_ptr iter();

    int count() const
    {
        return count_;
    }

private:
    const std::string buf_;
    const std::uint32_t count_;
};

}