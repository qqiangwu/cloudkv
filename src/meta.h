#pragma once

#include <vector>
#include <string>
#include <boost/serialization/vector.hpp>
#include "core.h"
#include "sstable/sstable.h"

namespace cloudkv {

namespace detail {

struct raw_meta {
    std::uint64_t committed_lsn;
    std::vector<std::string> sstables;

    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
        (void)version;

        ar & committed_lsn;
        ar & sstables;
    }
};

}

struct metainfo {
    std::uint64_t committed_lsn = 0;
    std::uint64_t next_lsn = 1;
    std::vector<sstable_ptr> sstables;

    detail::raw_meta to_raw() const
    {
        detail::raw_meta raw;

        raw.committed_lsn = committed_lsn;

        for (const auto& p: sstables) {
            raw.sstables.push_back(p->path());
        }

        return raw;
    }

    static metainfo from_raw(const detail::raw_meta& raw)
    {
        metainfo m;

        m.committed_lsn = raw.committed_lsn;
        m.next_lsn = m.committed_lsn + 1;

        for (const auto& p: raw.sstables) {
            m.sstables.push_back(std::make_shared<sstable>(p));
        }

        return m;
    }
};

};
