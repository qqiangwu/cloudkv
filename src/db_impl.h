#pragma once

#include "cloudkv/db.h"
#include "meta.h"

namespace cloudkv {

class db_impl : public kv_store {
public:
    db_impl(std::string_view name, options& opts);

    std::optional<std::string> query(std::string_view key) override;
    void batch_add(const std::vector<key_value>& key_values) override;
    void remove(std::string_view key) override;
    std::map<std::string, std::string> query_range(
        std::string_view start_key,
        std::string_view end_key, 
        int count = 128) override;

private:
    meta meta_;
};

}