#include <cstdint>
#include <fmt/core.h>
#include "util/coding.h"
#include "cloudkv/exception.h"
#include "sstable/sstable_builder.h"

using namespace std;
using namespace cloudkv;
using namespace leveldb;

sstable_builder::sstable_builder(std::ostream& out)
    : out_(out)
{
}

void sstable_builder::add(const internal_key& key, std::string_view value)
{
    if (!ctx_) {
        ctx_.emplace();

        auto& ctx = ctx_.value();
        ctx.key_min = key.user_key();
        ctx.key_max = key.user_key();
        ctx.count = 1;
    }

    auto& ctx = ctx_.value();
    ++ctx.count;
    if (key.user_key() < ctx.key_max) {
        throw std::invalid_argument{"key to add is not ordered"};
    }

    ctx.key_max = key.user_key();

    auto record = build_record_(key, value);
    out_.write(record.c_str(), record.size());
    if (!out_) {
        throw_system_error("add key value failed");
    }
}

void sstable_builder::done()
{
    if (!ctx_) {
        throw std::logic_error{"sstable_builder is empty"};
    }

    auto footer = build_footer_();
    out_.write(footer.c_str(), footer.size());
    out_.flush();
    if (!out_) {
        throw_system_error("flush sstable failed");
    }
}

void sstable_builder::encode_str_(std::string* buf, string_view s)
{
    PutFixed32(buf, s.size());
    *buf += s;
}

std::string sstable_builder::build_record_(const internal_key& key, std::string_view value)
{
    std::string buf;

    encode_str_(&buf, key.user_key());

    PutFixed64(&buf, key.seq());
    PutFixed32(&buf, std::uint32_t(key.type()));

    encode_str_(&buf, value);

    return buf;
}

std::string sstable_builder::build_footer_()
{
    std::string footer;

    encode_str_(&footer, ctx_->key_min);
    encode_str_(&footer, ctx_->key_max);
    
    PutFixed64(&footer, ctx_->count);
    PutFixed64(&footer, footer.size());

    return footer;
}