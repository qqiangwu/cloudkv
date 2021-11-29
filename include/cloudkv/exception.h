#pragma once

#include <stdexcept>
#include <cerrno>
#include <string>
#include <system_error>

namespace cloudkv {

struct data_corrupted : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct db_readonly : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

inline void throw_system_error(const std::string& msg)
{
    throw std::system_error(errno, std::system_category(), msg);
}

}