#include <filesystem>
#include <charconv>
#include <vector>
#include <fstream>
#include <spdlog/spdlog.h>
#include <range/v3/view.hpp>
#include <range/v3/action.hpp>
#include "replayer.h"
#include "util/fmt_std.h"
#include "cloudkv/exception.h"
#include "sstable/sstable_builder.h"
#include "memtable/redolog_reader.h"

using namespace cloudkv;
using namespace ranges;

namespace fs = std::filesystem;

namespace {

struct redo_ctx {
    path_t path;
    std::uint64_t lsn;
};

}

replay_result replayer::replay()
{
    std::vector<path_t> redologs;
    for (const auto& p: fs::directory_iterator(db_path_.redo_dir())) {
        redologs.push_back(p.path());
    }
    
    auto redolog_need_replay = redologs
    | views::transform([](const auto& p){
        const std::string_view raw = p.filename().native();
        std::uint64_t lsn;
        const auto r = std::from_chars(raw.begin(), raw.end(), lsn);
        if (r.ec == std::errc::invalid_argument) {
            spdlog::warn("invalid redolog {} found, ignored for safety", p);
            return redo_ctx { {}, 0 };
        }

        return redo_ctx { p, lsn };
    }) 
    | views::filter([committed_lsn = committed_lsn_](const redo_ctx& ctx){
        return ctx.lsn > committed_lsn;
    })
    | views::filter([](const redo_ctx& ctx){
        return fs::file_size(ctx.path) > 0;
    })
    | to<std::vector>() 
    | actions::sort([](const auto& x, const auto& y){
        return x.lsn < y.lsn;
    });

    replay_result res = { committed_lsn_ };

    for (const auto& ctx: redolog_need_replay) {
        auto sst = do_replay_(ctx.path);
        if (!sst) {
            spdlog::warn("skip bad redo {}", ctx.path);
            continue;
        }
        res.sstables.push_back(sst);
        res.replayed_lsn = ctx.lsn;
    }

    return res;
}

sstable_ptr replayer::do_replay_(const path_t& redopath)
{
    redolog_reader reader(redopath);
    auto sstpath = db_path_.next_sst_path();
    std::ofstream ofs(sstpath, std::ios::binary);
    if (!ofs) {
        throw db_corrupted{ fmt::format("cannot write sst file {}", sstpath) };
    }

    sstable_builder builder(ofs);
    int num_records = 0;

    while (auto entry = reader.next()) {
        const auto& logentry = entry.value();

        builder.add(internal_key{ logentry.key, logentry.op }, logentry.value);

        ++num_records;
    }

    if (!num_records) {
        return nullptr;
    }

    builder.done();

    return std::make_shared<sstable>(sstpath);
} 