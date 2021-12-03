#include <fstream>
#include <chrono>
#include <thread>
#include <filesystem>
#include <charconv>
#include <fmt/core.h>
#include <range/v3/view.hpp>
#include <spdlog/spdlog.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include "cloudkv/exception.h"
#include "db_impl.h"
#include "sstable/sstable_builder.h"
#include "util/fmt_std.h"

using namespace cloudkv;

namespace {

constexpr std::string_view meta_file = "meta";
constexpr std::string_view redo_dir = "redo";
constexpr std::string_view sst_dir = "sst";

}

namespace fs = std::filesystem;

db_impl::db_impl(std::string_view name, const options& opts)
    : cwd_(name),
      options_(opts),
      active_memtable_(std::make_shared<memtable>()),
      compaction_worker_(1),
      checkpoint_worker_(1)
{
    const auto db_exists = fs::exists(cwd_);

    if (opts.open_only && !db_exists) {
        throw std::invalid_argument{
            fmt::format("open db in open only mode, but {} not exist", cwd_)
        };
    }

    if (!db_exists) {
        fs::create_directory(cwd_);
        fs::create_directory(cwd_ / redo_dir);
        fs::create_directory(cwd_ / sst_dir);

        store_meta_(meta{});
    }

    if (!fs::is_regular_file(cwd_ / meta_file)) {
        throw db_corrupted{ fmt::format("invalid db {}, no meta file found", cwd_) };
    }
    
    // todo: load meta
    meta_ = load_meta_();

    // todo: replay


    redolog_ = std::make_shared<redolog>(next_redo_name_());
    ++meta_.next_lsn;
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
    write_batch batch;

    for (const auto& kv: key_values) {
        batch.add(kv.key, kv.value);
    }

    write_(batch);
}

void db_impl::remove(std::string_view key)
{
    write_batch batch;
    batch.remove(key);

    write_(batch);
}

std::map<std::string, std::string> db_impl::query_range(
    std::string_view start_key,
    std::string_view end_key, 
    int count)
{
    return {};
}

void db_impl::TEST_flush()
{
    spdlog::warn("flush memtables");
    auto checkpoint_done = [this]{
        std::lock_guard _(mut_);
        return immutable_memtable_ == nullptr;
    };

    using namespace std::chrono_literals;
    while (!checkpoint_done()) {
        std::this_thread::sleep_for(1ms);
    }
    spdlog::warn("immutable flushed");

    {
        std::lock_guard _(mut_);
        std::swap(immutable_memtable_, active_memtable_);
        if (immutable_memtable_) {
            issue_checkpoint_();
        }
    }

    while (!checkpoint_done()) {
        std::this_thread::sleep_for(1ms);
    } 
    spdlog::warn("active flushed");
}

path_t db_impl::next_redo_name_() const
{
    return cwd_ / "redo" / std::to_string(meta_.next_lsn);
}

void db_impl::store_meta_(const meta& meta)
{
    const auto mf = cwd_ / meta_file;
    std::ofstream ofs(mf);
    if (!ofs) {
        throw_system_error(fmt::format("store meta {} failed", mf));
    }

    boost::archive::text_oarchive oa(ofs);
    oa << meta.to_raw();
}

cloudkv::meta db_impl::load_meta_()
try {
    const auto mf = cwd_ / meta_file;
    std::ifstream ifs(mf);
    if (!ifs) {
        throw_system_error(fmt::format("open meta {} failed", mf));
    }

    boost::archive::text_iarchive ia(ifs);
    detail::raw_meta raw;
    ia >> raw;

    return cloudkv::meta::from_raw(raw);
} catch (boost::archive::archive_exception& e) {
    throw db_corrupted{ fmt::format("db corrupted: {}", e.what()) };
}

void db_impl::write_(const write_batch& writes)
{
    auto ctx = get_write_ctx_();
    auto& memtable = *ctx.memtable;

    {
        std::lock_guard _(tx_mut_);
        redolog_->write(writes);

        // commit
        try {
            for (const auto& [key, val, op]: writes) {
                memtable.add(op, key, val);
            }
        } catch (std::exception& e) {
            spdlog::critical("commit memtable failed after redo done: {}", e.what());
            std::terminate();
        }
    }

    try_schedule_checkpoint_(); 
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

    using namespace ranges;
    ctx.sstables = meta_.sstables | views::reverse | to<std::vector>();

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
        auto new_redo = std::make_shared<redolog>(next_redo_name_());

        // commit
        swap(immutable_memtable_, active_memtable_);
        swap(active_memtable_, new_memtable);
        ++meta_.next_lsn;

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
    spdlog::warn("run checkpoint task failed: {}, sleep for 1s", e.what());

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
    issue_checkpoint_();
}

void db_impl::do_checkpoint_impl_()
{
    using namespace fmt;
    using namespace std::chrono;

    const auto sst_path = cwd_ / sst_dir / format("sst.{}", steady_clock::now().time_since_epoch().count());
    const auto memtable = get_read_ctx_().memtables[1];
    
    std::ofstream ofs(sst_path, std::ios::binary);
    sstable_builder builder(ofs);

    for (const auto& [k, v]: memtable->items()) {
        builder.add(k, v);
    }

    builder.done();

    on_checkpoint_done_(std::make_shared<sstable>(sst_path));
}

void db_impl::on_checkpoint_done_(sstable_ptr sst)
{
    memtable_ptr empty;
    {
        std::lock_guard _(sys_mut_);
        auto meta = [this]{
            std::lock_guard _(mut_);
            return meta_;
        }();

        meta.sstables.push_back(sst);
        meta.committed_lsn += 1;

        spdlog::info("[meta] store, committed_lsn={}, sst={}", meta.committed_lsn, meta.sstables.size());
        store_meta_(meta);

        assert(meta.committed_lsn <= meta.next_lsn);

        spdlog::info("checkpoint done, {} added", sst->path());

        // commit
        std::lock_guard __(mut_);
        std::swap(immutable_memtable_, empty);
        std::swap(meta_, meta);
    }

    try_gc_();
}

void db_impl::try_gc_() noexcept
try {
    const auto committed_lsn = [this]{
        std::lock_guard _(mut_);
        return meta_.committed_lsn;
    }();

    for (const auto& p: std::filesystem::directory_iterator(cwd_ / redo_dir)) {
        const std::string_view raw = p.path().filename().native();
        std::uint64_t lsn;
        const auto r = std::from_chars(raw.begin(), raw.end(), lsn);
        if (r.ec == std::errc::invalid_argument) {
            spdlog::error("[gc] invalid redolog {} found, remove it", p.path());
            std::filesystem::remove(p.path());
            continue;
        }

        if (lsn > committed_lsn) {
            continue;
        }

        spdlog::info("[gc] remove legacy redo {}, committed_lsn={}", p.path(), committed_lsn);
        std::filesystem::remove(p.path());
    }
} catch (std::exception& e) {
    spdlog::warn("gc failed: {}", e.what());
}