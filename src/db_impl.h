#pragma once

#include <mutex>
#include <boost/thread/executors/basic_thread_pool.hpp>
#include "cloudkv/db.h"
#include "meta.h"
#include "memtable.h"

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

private:
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

private:
    const options options_;

    boost::basic_thread_pool checkpoint_worker_;
    boost::basic_thread_pool compaction_worker_;

    std::mutex mut_;

    meta meta_;
    memtable_ptr active_memtable_;
    memtable_ptr immutable_memtable_;
};

}