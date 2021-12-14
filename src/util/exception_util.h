#pragma once

#include <string>
#include <system_error>
#include <cerrno>

namespace cloudkv {

inline const char* current_exception_msg()
try {
    throw;
} catch (std::exception& e) {
    return e.what();
} catch (...) {
    return "unknown";
}

inline void throw_system_error(const std::string& msg)
{
    throw std::system_error(errno, std::system_category(), msg);
}

}