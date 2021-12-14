#include <charconv>
#include <spdlog/spdlog.h>
#include "task/gc_task.h"
#include "util/fmt_std.h"

using namespace cloudkv;

namespace fs = std::filesystem;

gc_task::gc_task(const path_conf& db_path, gc_root& gcroot, std::uint64_t committed_file_id)
    : db_path_(db_path), gc_root_(gcroot), committed_file_id_(committed_file_id)
{
}

void gc_task::run()
{
    // redolog
    for (const auto& p: fs::directory_iterator(db_path_.redo_dir())) {
        const std::string_view raw = p.path().filename().native();
        std::uint64_t file_id = 0;
        const auto r = std::from_chars(raw.begin(), raw.end(), file_id);
        if (r.ec == std::errc::invalid_argument) {
            spdlog::warn("[gc] invalid redolog {} found, ignored for safety", p.path());
            continue;
        }

        if (file_id > committed_file_id_) {
            continue;
        }

        spdlog::info("[gc] remove legacy redo {}, committed_file_id={}", p.path(), committed_file_id_);
        fs::remove(p.path());
    }

    // sst
    for (const auto& p: fs::directory_iterator(db_path_.sst_dir())) {
        if (gc_root_.is_reachable(p.path())) {
            continue;
        }

        spdlog::info("[gc] remove sst {}", p.path());
        fs::remove(p.path());
    }
}