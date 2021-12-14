#pragma once

#include <mutex>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <boost/noncopyable.hpp>
#include "sstable/sstable.h"
#include "core.h"

namespace cloudkv {

class gc_root : private boost::noncopyable {
public:
    bool is_reachable(const path_t& sst);

    // not atomic
    template <class Rng>
    void add(const Rng& rng)
    {
        for (const auto& p: rng) {
            add(p);
        }
    }

    void add(sstable_ptr sst);
    void add_temporary(const path_t& sst);

    void remove(sstable_ptr sst);
    void remove_temporary(const path_t& sst);

private:
    struct path_hash {
        std::size_t operator()(const path_t& p) const noexcept
        {
            return std::filesystem::hash_value(p);
        }
    };

    std::mutex mut_;
    std::unordered_map<path_t, std::weak_ptr<sstable>, path_hash> reachable_;
    std::unordered_set<path_t, path_hash> temporary_reachable_;
};

// for checkpoints and compactions
class temporary_gc_root : private boost::noncopyable {
public:
    explicit temporary_gc_root(gc_root& root)
        : gc_root_(root)
    {}

    ~temporary_gc_root()
    {
        for (const auto& p: temporary_files_) {
            gc_root_.remove_temporary(p);
        }
    }

    void add(const path_t& p)
    {
        gc_root_.add_temporary(p);
        temporary_files_.push_back(p);
    }

private:
    gc_root& gc_root_;
    std::vector<path_t> temporary_files_;
};

}