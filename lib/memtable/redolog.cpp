#include <cassert>
#include <fmt/core.h>
#include "util/format.h"
#include "util/exception_util.h"
#include "memtable/redolog.h"

using namespace cloudkv;

redolog::redolog(const path_t& p, std::uint64_t buffer_size)
{
    assert(buffer_size > 0);

    out_.open(p, std::ios::binary);
    if (!out_) {
        throw_system_error(fmt::format("open {} for redo failed"));
    }

    out_.exceptions(std::ios::failbit | std::ios::badbit);
    buf_.reserve(buffer_size);
}

void redolog::write(std::string_view record)
{
    PutFixed32(&buf_, record.size());
    buf_.append(record.begin(), record.end());

    out_.write(buf_.data(), buf_.size());
    out_.flush();

    buf_.clear();
}