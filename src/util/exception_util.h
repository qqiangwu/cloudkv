#include <exception>

namespace cloudkv {

inline const char* current_exception_msg()
try {
    throw;
} catch (std::exception& e) {
    return e.what();
} catch (...) {
    return "unknown";
}

}