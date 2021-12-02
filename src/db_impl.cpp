#include <fstream>
#include <chrono>
#include <fmt/core.h>
#include <range/v3/view.hpp>
#include <spdlog/spdlog.h>
#include "db_impl.h"
#include "sstable/sstable_builder.h"

using namespace cloudkv;
using namespace ranges;

db_impl::db_impl(std::string_view name, const options& opts)
    : options_(opts), 
      checkpoint_worker_(1),
      compaction_worker_(1),
      active_memtable_(std::make_shared<memtable>())
{
    /* empty */
}

std::optional<std::string> db_impl::query(std::string_view key)
{
    auto ctx = get_read_ctx_();

    for (const auto& memtable: ctx.memtables) {
        auto r = memtable->query(key);
        if (r) {
            return r.value().key.is_deleted()? nullptr: std::optional(std::move(r.value().value));
        }
    }

    for (const auto& sst: ctx.sstables) {
        auto r = sst->query(key);
        if (r) {
            return r.value().key.is_deleted()? nullptr: std::optional(std::move(r.value().value));
        }
    }

    return std::nullopt;
}

void db_impl::batch_add(const std::vector<key_value>& key_values)
{
    auto ctx = get_write_ctx_();
    auto& memtable = *ctx.memtable;

    for (const auto& kv: key_values) {
        memtable.add(kv.key, kv.value);
    }

    try_schedule_checkpoint_();
}

void db_impl::remove(std::string_view key)
{
    auto ctx = get_write_ctx_();
    
    ctx.memtable->remove(key);

    try_schedule_checkpoint_();
}

std::map<std::string, std::string> db_impl::query_range(
    std::string_view start_key,
    std::string_view end_key, 
    int count)
{
    return {};
}

db_impl::write_ctx db_impl::get_write_ctx_()
{
    std::lock_guard _(mut_);
    return { active_memtable_ };
}

db_impl::read_ctx db_impl::get_read_ctx_()
{
    std::lock_guard _(mut_);

    read_ctx ctx;
    ctx.memtables.push_back(active_memtable_);
    
    if (immutable_memtable_) {
        ctx.memtables.push_back(immutable_memtable_);
    }

    ctx.sstables = meta_.sstables_ | views::reverse | to<std::vector>();

    return ctx;
}

void db_impl::try_schedule_checkpoint_() noexcept
{
    {
        std::lock_guard _(mut_);
        if (immutable_memtable_) {
            return;
        }

        if (active_memtable_->bytes_used() < options_.write_buffer_size) {
            return;
        }

        auto new_memtable = std::make_shared<memtable>();

        swap(immutable_memtable_, active_memtable_);
        swap(active_memtable_, new_memtable);

        spdlog::info("checkpoint issued");
    }

    issue_checkpoint_();
}

void db_impl::issue_checkpoint_() noexcept
try {
    checkpoint_worker_.submit([this]{
        do_checkpoint_();
    });
} catch (boost::sync_queue_is_closed&) {
    spdlog::warn("checkpoint worker is closed, stop checkpoint");
} catch (std::exception& e) {
    spdlog::critical("(exit) submit checkpoint task failed: {}", e.what());

    std::terminate();
}

// bg task
void db_impl::do_checkpoint_() noexcept
try {
    do_checkpoint_impl_();
} catch (std::exception& e) {
    spdlog::warn("run checkpoint task failed: {}", e.what());

    issue_checkpoint_();    
}

void db_impl::do_checkpoint_impl_()
{
    using namespace fmt;
    using namespace std::chrono;

    const auto sst_path = format("sst.{}", steady_clock::now().time_since_epoch().count());
    const auto memtable = get_read_ctx_().memtables[1];
    
    std::fstream ifs(sst_path, std::ios::binary);
    sstable_builder builder(ifs);

    for (const auto& [k, v]: memtable->items()) {
        builder.add(k, v);
    }

    builder.done();

    on_checkpoint_done_(std::make_shared<sstable>(sst_path));
}

void db_impl::on_checkpoint_done_(sstable_ptr sst)
{
    std::lock_guard _(mut_);
    auto meta = meta_;
    meta.sstables_.push_back(sst);

    spdlog::info("checkpoint done, {} added", sst->path().native());
    std::swap(meta_, meta);
}