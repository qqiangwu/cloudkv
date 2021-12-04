#pragma once

#include "path_conf.h"
#include "sstable/sstable.h"

namespace cloudkv {

struct replay_result {
    std::uint64_t replayed_lsn;
    std::vector<sstable_ptr> sstables;
};

class replayer {
public:
    replayer(const path_conf& db, std::uint64_t committed_lsn)
        : db_path_(db), committed_lsn_(committed_lsn)
    {
    }

    replay_result replay();

private:
    sstable_ptr do_replay_(const path_t& p);

private:
    const path_conf& db_path_;
    const std::uint64_t committed_lsn_;
};

}