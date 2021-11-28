#pragma once

#include <stdexcept>

namespace cloudkv {

struct data_corrupted : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct db_readonly : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

}