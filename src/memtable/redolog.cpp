#include <cassert>
#include <fmt/core.h>
#include "cloudkv/exception.h"
#include "util/format.h"
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

void redolog::write(const write_batch& batch)
{
    constexpr int header_size = sizeof(uint32_t);

    buf_.clear();

    for (const auto& [key, value, op]: batch) {
        const auto old_size = buf_.size();
        
        buf_.append(header_size, 0);

        PutFixed32(&buf_, std::uint32_t(op));
        encode_str(&buf_, key);
        encode_str(&buf_, value);

        EncodeFixed32(buf_.data() + old_size, buf_.size() - old_size - header_size);
    }

    out_.write(buf_.data(), buf_.size());
    out_.flush();
}

void redolog::flush()
{
    out_.flush();
}