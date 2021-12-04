#include <charconv>
#include <spdlog/spdlog.h>
#include "task/gc_task.h"
#include "util/fmt_std.h"

using namespace cloudkv;

gc_task::gc_task(const path_conf& db_path, std::uint64_t committed_lsn)
    : db_path_(db_path), committed_lsn_(committed_lsn)
{
}

// todo: how to file gc
void gc_task::run()
{
    for (const auto& p: std::filesystem::directory_iterator(db_path_.redo_dir())) {
        const std::string_view raw = p.path().filename().native();
        std::uint64_t lsn;
        const auto r = std::from_chars(raw.begin(), raw.end(), lsn);
        if (r.ec == std::errc::invalid_argument) {
            spdlog::warn("[gc] invalid redolog {} found, ignored for safety", p.path());
            continue;
        }

        if (lsn > committed_lsn_) {
            continue;
        }

        spdlog::info("[gc] remove legacy redo {}, committed_lsn={}", p.path(), committed_lsn_);
        std::filesystem::remove(p.path());
    }
}