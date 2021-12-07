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
#include "memtable/redolog_reader.h"
#include "memtable/memtable.h"
#include "task/checkpoint_task.h"

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
            std::uint64_t lsn = 0;
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
    auto mt = std::make_shared<memtable>();
    while (auto entry = reader.next()) {
        const auto& logentry = entry.value();

        mt->add(logentry.op, logentry.key, logentry.value);
    }

    if (mt->bytes_used() == 0) {
        return nullptr;
    }

    sstable_ptr result;
    checkpoint_task(db_path_, std::move(mt), [&result](const auto& sst){
        result = sst;
    }).run();

    return result;
}
