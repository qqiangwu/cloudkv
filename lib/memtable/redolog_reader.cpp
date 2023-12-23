#include <cassert>
#include "util/fmt_std.h"
#include "util/format.h"
#include "util/exception_util.h"
#include "memtable/redolog_reader.h"

using namespace cloudkv;

redolog_reader::redolog_reader(const path_t& p)
    : ifs_(p, std::ios::binary)
{
    if (!ifs_) {
        throw_system_error(fmt::format("open redolog {} failed", p));
    }
}

// todo: whatif redolog corrupted
std::optional<std::string_view> redolog_reader::next()
{
    assert(!ifs_.eof());

    std::uint32_t record_size;
    ifs_.read(reinterpret_cast<char*>(&record_size), sizeof(record_size));
    if (!ifs_) {
        return std::nullopt;
    }

    buf_.resize(record_size);
    ifs_.read(buf_.data(), buf_.size());
    if (!ifs_) {
        return std::nullopt;
    }

    return buf_;
}