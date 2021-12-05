#pragma once

#include <vector>
#include "iter.h"

namespace cloudkv {

inline std::vector<internal_key_value> dump_all(const iter_ptr& iter)
{
    std::vector<internal_key_value> results;

    while (!iter->is_eof()) {
        results.push_back(iter->next());
    }

    return results;
}

}