#include <filesystem>
#include <charconv>
#include <vector>
#include <fstream>
#include <spdlog/spdlog.h>
#include <range/v3/view.hpp>
#include <range/v3/action.hpp>
#include "replayer.h"
#include "gc_root.h"
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
    std::uint64_t file_id;
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
            std::uint64_t file_id = 0;
            const auto r = std::from_chars(raw.begin(), raw.end(), file_id);
            if (r.ec == std::errc::invalid_argument) {
                spdlog::warn("invalid redolog {} found, ignored for safety", p);
                return redo_ctx { {}, 0 };
            }

            return redo_ctx { p, file_id };
        })
        | views::filter([committed_file_id = committed_file_id_](const redo_ctx& ctx){
            return ctx.file_id > committed_file_id;
        })
        | views::filter([](const redo_ctx& ctx){
            return fs::file_size(ctx.path) > 0;
        })
        | to<std::vector>()
        | actions::sort([](const auto& x, const auto& y){
            return x.file_id < y.file_id;
        });

    replay_result res = { committed_file_id_ };

    for (const auto& ctx: redolog_need_replay) {
        auto sst = do_replay_(ctx.path);
        if (!sst) {
            spdlog::warn("skip bad redo {}", ctx.path);
            continue;
        }
        res.sstables.push_back(sst);
        res.replayed_file_id = ctx.file_id;
    }

    return res;
}

sstable_ptr replayer::do_replay_(const path_t& redopath)
{
    gc_root gc;
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
    checkpoint_task({
        db_path_,
        file_id_alloc_,
        gc,
        std::move(mt),
        [&result](const auto& sst){
            result = sst;
        }
    }).run();

    return result;
}
