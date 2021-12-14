#include <fstream>
#include <filesystem>
#include <boost/serialization/vector.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <scope_guard.hpp>
#include "cloudkv/exception.h"
#include "util/exception_util.h"
#include "util/fmt_std.h"
#include "meta.h"

using namespace cloudkv;

namespace fs = std::filesystem;

namespace {

struct raw_meta {
    std::uint64_t committed_file_id;
    std::uint64_t next_file_id;
    std::vector<std::string> sstables;

    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
        (void)version;

        ar & committed_file_id;
        ar & next_file_id;
        ar & sstables;
    }

    static raw_meta from_metainfo(const metainfo& info)
    {
        assert(info.committed_file_id < info.next_file_id);

        raw_meta raw;

        raw.committed_file_id = info.committed_file_id;
        raw.next_file_id = info.next_file_id;

        for (const auto& p: info.sstables) {
            raw.sstables.push_back(p->path());
        }

        return raw;
    }

    metainfo to_metainfo() const
    {
        assert(committed_file_id < next_file_id);

        metainfo m;

        m.committed_file_id = committed_file_id;
        m.next_file_id = next_file_id;

        for (const auto& p: sstables) {
            m.sstables.push_back(std::make_shared<sstable>(p));
        }

        return m;
    }
};

}

metainfo metainfo::load(const path_t& p)
try {
    std::ifstream ifs(p);
    if (!ifs) {
        throw_system_error(fmt::format("open meta {} failed", p));
    }

    boost::archive::text_iarchive ia(ifs);
    raw_meta raw;
    ia >> raw;

    return raw.to_metainfo();
} catch (boost::archive::archive_exception& e) {
    throw db_corrupted{ fmt::format("db {} corrupted: {}, load metainfo failed", p, e.what()) };
}

void metainfo::store(const path_t& p)
{
    const path_t tmp = p.native() + "~";
    std::ofstream ofs(tmp);
    if (!ofs) {
        throw_system_error(fmt::format("store meta {} failed in create temporary copy", p));
    }
    SCOPE_FAIL {
        fs::remove(tmp);
    };

    boost::archive::text_oarchive oa(ofs);
    oa << raw_meta::from_metainfo(*this);
    ofs.close();

    // commit
    fs::rename(tmp, p);
}
