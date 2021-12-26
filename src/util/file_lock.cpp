#include <fstream>
#include "util/exception_util.h"
#include "util/fmt_std.h"
#include "util/file_lock.h"

using namespace cloudkv;

file_lock::file_lock(const path_t& p)
{
    if (!fs::exists(p)) {
        std::ofstream ofs(p);
        if (!ofs) {
            throw_io_error(fmt::format("create lock file failed", p));
        }
    }

    lock_ = boost::interprocess::file_lock(p.c_str());

    if (!lock_.try_lock()) {
        throw db_opened_by_others(fmt::format("db opened by others, lock={}", p));
    }
}