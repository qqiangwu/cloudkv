#include <fstream>
#include <chrono>
#include <thread>
#include <filesystem>
#include <fmt/core.h>
#include <range/v3/view.hpp>
#include <range/v3/algorithm.hpp>
#include <spdlog/spdlog.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <scope_guard.hpp>
#include "cloudkv/exception.h"
#include "db_impl.h"
#include "replayer.h"
#include "util/fmt_std.h"
#include "task/gc_task.h"
#include "task/checkpoint_task.h"
#include "task/compaction_task.h"

using namespace cloudkv;
using namespace std::chrono_literals;

namespace fs = std::filesystem;

using std::nullopt;

db_impl::db_impl(std::string_view name, const options& opts)
    : options_(opts),
      db_path_(name),
      active_memtable_(std::make_shared<memtable>())
{
    const auto db_exists = fs::exists(db_path_.root());

    if (opts.open_only && !db_exists) {
        throw std::invalid_argument{
            fmt::format("open db in open only mode, but {} not exist", db_path_.root())
        };
    }

    if (!db_exists) {
        fs::create_directory(db_path_.root());
        fs::create_directory(db_path_.redo_dir());
        fs::create_directory(db_path_.sst_dir());

        store_meta_(metainfo{});
    }

    if (!fs::is_regular_file(db_path_.meta_info())) {
        throw db_corrupted{ fmt::format("invalid db {}, no meta file found", db_path_.root()) };
    }

    meta_ = load_meta_();
    spdlog::info("[startup] load meta: root={} committed_lsn={}", db_path_.root(), meta_.committed_lsn);

    for (const auto& sst: meta_.sstables) {
        spdlog::info("[startup] load meta: sst={}", sst->path());
    }

    spdlog::info("[startup] replay");
    auto replay_res = replayer(db_path_, meta_.committed_lsn).replay();
    if (replay_res.replayed_lsn > meta_.committed_lsn) {
        spdlog::info("[startup] replay done: lsn={}, sst={}", replay_res.replayed_lsn, replay_res.sstables.size());

        auto& sst = replay_res.sstables;
        meta_.committed_lsn = replay_res.replayed_lsn;
        meta_.sstables.insert(meta_.sstables.end(), sst.begin(), sst.end());
        store_meta_(meta_);
        try_gc_();
    }

    meta_.next_lsn = meta_.committed_lsn + 1;
    redolog_ = std::make_shared<redolog>(db_path_.redo_path(meta_.next_lsn));
    ++meta_.next_lsn;
}

std::optional<std::string> db_impl::query(std::string_view key)
{
    auto ctx = get_read_ctx_();

    for (const auto& memtable: ctx.memtables) {
        auto r = memtable->query(key);
        if (r) {
            return r.value().key.is_deleted()? nullopt: std::optional(std::move(r.value().value));
        }
    }

    for (const auto& sst: ctx.sstables) {
        auto r = sst->query(key);
        if (r) {
            return r.value().key.is_deleted()? nullopt: std::optional(std::move(r.value().value));
        }
    }

    return nullopt;
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
    }

    if (!checkpoint_done()) {
        try_checkpoint_();
    }

    while (!checkpoint_done()) {
        std::this_thread::sleep_for(1ms);
    }
    spdlog::warn("active flushed");
}

void db_impl::store_meta_(const metainfo& meta)
{
    const auto mf = db_path_.meta_info();
    std::ofstream ofs(mf);
    if (!ofs) {
        throw_system_error(fmt::format("store meta {} failed", mf));
    }

    boost::archive::text_oarchive oa(ofs);
    oa << meta.to_raw();
}

metainfo db_impl::load_meta_()
try {
    std::ifstream ifs(db_path_.meta_info());
    if (!ifs) {
        throw_system_error(fmt::format("open meta {} failed", db_path_.meta_info()));
    }

    boost::archive::text_iarchive ia(ifs);
    detail::raw_meta raw;
    ia >> raw;

    return metainfo::from_raw(raw);
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
        auto new_redo = std::make_shared<redolog>(db_path_.redo_path(meta_.next_lsn));

        // commit
        swap(immutable_memtable_, active_memtable_);
        swap(active_memtable_, new_memtable);
        swap(redolog_, new_redo);
        ++meta_.next_lsn;

        spdlog::info("checkpoint issued");
    }

    try_checkpoint_();
}

void db_impl::try_checkpoint_() noexcept
try {
    auto memtable = [this]{
        std::lock_guard _(mut_);
        return immutable_memtable_;
    }();
    auto task = std::make_unique<checkpoint_task>(db_path_, memtable, [this](sstable_ptr sst){
        on_checkpoint_done_(sst);
    });

    // ignore result
    task_mgr_.submit(std::move(task), [this](const std::exception& e) noexcept {
        spdlog::warn("run checkpoint task failed [{}], reschedule after for 1s", e.what());
        std::this_thread::sleep_for(1s);
        try_checkpoint_();
    });
} catch (std::exception& e) {
    spdlog::critical("(exit) submit checkpoint task failed: {}", e.what());

    // terminate now to avoid complicated retry logic
    std::terminate();
}

void db_impl::try_compaction_() noexcept
try {
    if (!need_compaction_()) {
        return;
    }
    if (compaction_running_.test_and_set()) {
        return;
    }
    SCOPE_FAIL {
        compaction_running_.clear();
    };

    auto sstables = [this]{
        std::lock_guard _(mut_);
        return meta_.sstables;
    }();
    auto callback = [this](const std::vector<sstable_ptr>& added, const std::vector<sstable_ptr>& removed){
        on_compaction_done_(added, removed);
    };
    auto task = std::make_unique<compaction_task>(db_path_, options_, std::move(sstables), callback);

    task_mgr_.submit(std::move(task), [this](const std::exception& e) noexcept {
        spdlog::warn("run compaction task failed [{}]", e.what());
        compaction_running_.clear();
    });
} catch (std::exception& e) {
    spdlog::error("try compaction failed: {}, ignored", e.what());
}

void db_impl::on_checkpoint_done_(sstable_ptr sst)
{
    std::vector<sstable_ptr> added { sst };
    std::vector<sstable_ptr> removed;
    const std::uint64_t committed_lsn = 1 + [this]{
        std::lock_guard _(mut_);
        return meta_.committed_lsn;
    }();

    meta_update_args args {
        added,
        removed,
        committed_lsn
    };
    update_meta_(args);

    // commit
    using namespace std;
    memtable_ptr empty;
    std::lock_guard __(mut_);
    swap(immutable_memtable_, empty);
}

void db_impl::on_compaction_done_(const std::vector<sstable_ptr>& added, const std::vector<sstable_ptr>& removed)
{
    SCOPE_EXIT {
        compaction_running_.clear();
    };

    meta_update_args args {
        added,
        removed,
        0
    };
    update_meta_(args);
}

void db_impl::update_meta_(const meta_update_args& args)
{
    {
        std::lock_guard _(sys_mut_);
        auto meta = [this]{
            std::lock_guard _(mut_);
            return meta_;
        }();
        auto& sstables = meta.sstables;
        const auto& added = args.added;
        const auto& removed = args.removed;

        // TODO: how to maintain sst orders?
        const auto old_sst_count = sstables.size();
        if (removed.empty()) {
            sstables.insert(sstables.end(), added.begin(), added.end());
        } else {
            if (sstables.size() + added.size() < removed.size()) {
                // code bug
                throw std::logic_error{
                    fmt::format("removed too many sstables: old={}, added={}, removed={}",
                        old_sst_count,
                        added.size(),
                        removed.size())
                };
            }

            const auto remove_from = ranges::find(sstables, removed[0]);
            if (remove_from == sstables.end()) {
                // code bug
                throw std::logic_error{ "found unknown file after compaction" };
            }

            const auto remove_to = remove_from + removed.size();
            if (!std::equal(remove_from, remove_to, removed.begin(), removed.end())) {
                // code bug
                throw std::logic_error{ "found unknown file after compaction" };
            }

            std::vector<sstable_ptr> new_sstables;
            new_sstables.reserve(old_sst_count + added.size() - removed.size());
            new_sstables.insert(new_sstables.end(), sstables.begin(), remove_from);
            new_sstables.insert(new_sstables.end(), added.begin(), added.end());
            new_sstables.insert(new_sstables.end(), remove_to, sstables.end());

            sstables.swap(new_sstables);
        }

        if (args.committed_lsn > meta.committed_lsn) {
            meta.committed_lsn = args.committed_lsn;
        }

        spdlog::info("[meta] store, committed_lsn={}, sst={}", meta.committed_lsn, meta.sstables.size());
        store_meta_(meta);

        assert(meta.committed_lsn <= meta.next_lsn);

        spdlog::info("compaction done, old={} sst, new={} sst", old_sst_count, sstables.size());

        // commit
        std::lock_guard __(mut_);
        using namespace std;
        swap(meta_.committed_lsn, meta.committed_lsn);
        swap(meta_.sstables, meta.sstables);
    }

    try_gc_();
    try_compaction_();
}

void db_impl::try_gc_() noexcept
try {
    const auto committed_lsn = [this]{
        std::lock_guard _(mut_);
        return meta_.committed_lsn;
    }();

    auto task = std::make_unique<gc_task>(db_path_, committed_lsn);

    // ignore result
    task_mgr_.submit(std::move(task));
} catch (std::exception& e) {
    spdlog::warn("gc failed: {}", e.what());
}

bool db_impl::need_compaction_()
{
    std::lock_guard _(mut_);
    const auto& sstables = meta_.sstables;

    if (sstables.size() < options_.compaction_water_mark) {
        return false;
    }

    // todo: refine me
    return ranges::any_of(sstables, [this](const auto& sst){
        return sst->size_in_bytes() < options_.sstable_size;
    });
}