#pragma once

#include <mutex>
#include <boost/thread/executors/basic_thread_pool.hpp>
#include "cloudkv/db.h"
#include "memtable/memtable.h"
#include "memtable/redolog.h"
#include "meta.h"

namespace cloudkv {

class db_impl : public kv_store {
public:
    db_impl(std::string_view name, const options& opts);

    std::optional<std::string> query(std::string_view key) override;

    void batch_add(const std::vector<key_value>& key_values) override;
    
    void remove(std::string_view key) override;
    
    std::map<std::string, std::string> query_range(
        std::string_view start_key,
        std::string_view end_key, 
        int count = 128) override;

public:
    // for tests
    void TEST_flush();

private:
    // helpers
    path_t next_redo_name_() const;

private:
    void store_meta_(const meta& meta);
    meta load_meta_();

private:
    void write_(const write_batch& batch);

    struct write_ctx {
        memtable_ptr memtable;
    };
    write_ctx get_write_ctx_();

    struct read_ctx {
        std::vector<memtable_ptr> memtables;
        std::vector<sstable_ptr> sstables;
    };
    read_ctx get_read_ctx_();

    void try_schedule_checkpoint_() noexcept;

    void issue_checkpoint_() noexcept;
    void do_checkpoint_() noexcept;
    void do_checkpoint_impl_();
    void on_checkpoint_done_(sstable_ptr sst);

    void try_gc_() noexcept;

private:
    const path_t cwd_;
    const options options_;

    std::mutex sys_mut_;  // for meta mutation
    std::mutex mut_;      // for db state
    meta meta_;

    std::mutex tx_mut_;   // for kv write operation
    redolog_ptr redolog_;
    memtable_ptr active_memtable_;
    memtable_ptr immutable_memtable_;

    boost::basic_thread_pool compaction_worker_;
    boost::basic_thread_pool checkpoint_worker_;
};

}