#include "gc_root.h"

using namespace cloudkv;

bool gc_root::is_reachable(const path_t& sst)
{
    std::lock_guard _(mut_);

    if (temporary_reachable_.find(sst) != temporary_reachable_.end()) {
        return true;
    }

    auto iter = reachable_.find(sst);
    if (iter == reachable_.end()) {
        return false;
    }

    if (iter->second.expired()) {
        reachable_.erase(iter);
        return false;
    }

    return true;
}

void gc_root::add(sstable_ptr sst)
{
    std::lock_guard _(mut_);
    reachable_.emplace(sst->path(), sst);
}

void gc_root::add_temporary(const path_t& sst)
{
    std::lock_guard _(mut_);
    temporary_reachable_.insert(sst);
}

void gc_root::remove(sstable_ptr sst)
{
    std::lock_guard _(mut_);
    reachable_.erase(sst->path());
}

void gc_root::remove_temporary(const path_t& sst)
{
    std::lock_guard _(mut_);
    temporary_reachable_.erase(sst);
}