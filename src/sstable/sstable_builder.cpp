#include <cassert>
#include <cstdint>
#include <fmt/core.h>
#include "util/coding.h"
#include "util/fmt_std.h"
#include "util/exception_util.h"
#include "sstable/format.h"
#include "sstable/sstable_builder.h"

using namespace std;
using namespace cloudkv;

sstable_builder::sstable_builder(std::ostream& out)
    : out_(out)
{
    if (!out_) {
        throw_system_error(fmt::format("create sst failed"));
    }

    out_.exceptions(std::ios::failbit | std::ios::badbit);
}

sstable_builder::sstable_builder(const path_t& p)
    : path_(p),
      buf_(std::make_unique<std::ofstream>(p, std::ios::binary)),
      out_(*buf_.get())
{
    if (!out_) {
        throw_system_error(fmt::format("create sst {} failed", path_));
    }

    out_.exceptions(std::ios::failbit | std::ios::badbit);
}

void sstable_builder::add(const internal_key& key, std::string_view value)
{
    assert(key_max_.empty() || key_max_ <= key.user_key());

    auto record = build_record_(key, value);
    out_.write(record.c_str(), record.size());
    if (!out_) {
        throw_system_error("add key value failed");
    }

    size_in_bytes_ += record.size();
    if (key_max_.empty()) {
        assert(key_min_.empty());
        key_min_ = key.user_key();
    }

    key_max_ = key.user_key();
    ++count_;
}

void sstable_builder::done()
{
    assert(count_ > 0);
    assert(!key_min_.empty());
    assert(!key_max_.empty());

    auto footer = build_footer_();
    out_.write(footer.c_str(), footer.size());
    out_.flush();
    if (!out_) {
        throw_system_error("flush sstable failed");
    }
}

std::string sstable_builder::build_record_(const internal_key& key, std::string_view value)
{
    std::string buf;

    encode_str(&buf, key.user_key());

    PutFixed32(&buf, std::uint32_t(key.type()));

    encode_str(&buf, value);

    return buf;
}

std::string sstable_builder::build_footer_()
{
    std::string footer;

    encode_str(&footer, key_min_);
    encode_str(&footer, key_max_);

    PutFixed32(&footer, count_);
    PutFixed32(&footer, footer.size());

    return footer;
}