#pragma once

#include <atomic>
#include <mutex>
#include <boost/thread/executors/basic_thread_pool.hpp>
#include "cloudkv/db.h"
#include "memtable/memtable.h"
#include "memtable/redolog.h"
#include "task/task_manager.h"
#include "meta.h"
#include "file_id_allocator.h"
#include "gc_root.h"
#include "path_conf.h"
#include "batch_executor.h"

namespace cloudkv {

class db_impl : public kv_store {
public:
    db_impl(std::string_view name, const options& opts);

    std::optional<std::string> query(std::string_view key) override;

    void batch_add(const std::vector<key_value>& key_values) override;

    void remove(std::string_view key) override;

    iter_ptr iter() override;

public:
    // for tests
    void TEST_flush();

private:
    void write_(const write_batch& batch);

    void make_room_();
    void commit_(const write_batch& batch);

    struct read_ctx {
        std::vector<memtable_ptr> memtables;
        std::vector<sstable_ptr> sstables;
    };
    read_ctx get_read_ctx_();

    void try_checkpoint_() noexcept;
    void try_compaction_() noexcept;
    void try_gc_() noexcept;

    void on_checkpoint_done_(sstable_ptr sst);
    void on_compaction_done_(const std::vector<sstable_ptr>& added, const std::vector<sstable_ptr>& removed);

    struct meta_update_args {
        const std::vector<sstable_ptr>& added;
        const std::vector<sstable_ptr>& removed;
        std::uint64_t committed_file_id;
    };
    void update_meta_(const meta_update_args&);

    bool need_compaction_();

private:
    const options options_;
    const path_conf db_path_;

    std::mutex sys_mut_;  // for meta mutation
    std::mutex mut_;      // for db state
    metainfo meta_;
    file_id_allocator file_id_alloc_;
    gc_root gc_root_;

    redolog_ptr redolog_;
    memtable_ptr active_memtable_;
    memtable_ptr immutable_memtable_;

    std::atomic_flag compaction_running_ = ATOMIC_FLAG_INIT;

    task_manager task_mgr_;
    batch_executor write_executor_;
};

}