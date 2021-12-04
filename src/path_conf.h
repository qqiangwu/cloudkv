#pragma once

#include <string>
#include <fmt/core.h>
#include "core.h"

namespace cloudkv {

class path_conf {
    static constexpr std::string_view META_INFO = "meta";
    static constexpr std::string_view REDO_DIR = "redo";
    static constexpr std::string_view SST_DIR = "sst";

public:
    explicit path_conf(const path_t& p)
        : db_path_(p),
          meta_info_(p / META_INFO),
          redo_dir_(p / REDO_DIR),
          sst_dir_(p / SST_DIR)
    {}

    const path_t& db() const
    {
        return db_path_;
    }

    const path_t& meta_info() const
    {
        return meta_info_;
    }

    const path_t& redo_dir() const
    {
        return redo_dir_;
    }

    const path_t& sst_dir() const
    {
        return sst_dir_;
    }

    path_t redo_path(std::uint64_t file_id) const
    {
        return redo_dir() / std::to_string(file_id);
    }

    path_t sst_path(std::uint64_t file_id) const
    {
        return sst_dir() / fmt::format("sst.{}", file_id);
    }

private:
    const path_t db_path_;
    const path_t meta_info_;
    const path_t redo_dir_;
    const path_t sst_dir_;
};

}