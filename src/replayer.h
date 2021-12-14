#pragma once

#include "sstable/sstable.h"
#include "path_conf.h"
#include "file_id_allocator.h"
#include "gc_root.h"

namespace cloudkv {

struct replay_result {
    std::uint64_t replayed_file_id;
    std::vector<sstable_ptr> sstables;
};

class replayer {
public:
    replayer(const path_conf& db, file_id_allocator& id_alloc, std::uint64_t committed_file_id)
        : db_path_(db),
          file_id_alloc_(id_alloc),
          committed_file_id_(committed_file_id)
    {
    }

    replay_result replay();

private:
    sstable_ptr do_replay_(const path_t& p);

private:
    const path_conf& db_path_;
    file_id_allocator& file_id_alloc_;
    const std::uint64_t committed_file_id_;
};

}