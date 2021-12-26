#pragma once

#include <stdexcept>
#include <system_error>

namespace cloudkv {

// the db files are corrupted
struct db_corrupted : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

// the content of db files are corrupted
struct data_corrupted : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct db_opened_by_others: public std::runtime_error {
    using std::runtime_error::runtime_error;
};

// errors when operates on files
struct io_error: public std::system_error {
    using std::system_error::system_error;
};

}