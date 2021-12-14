#pragma once

#include <memory>
#include <string>
#include <stdexcept>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include "cloudkv/db.h"
#include "fmt/core.h"

namespace cloudkv {

class LevelDBImpl : public kv_store {
public:
    LevelDBImpl(const options&, const std::string& name)
    {
        using namespace leveldb;

        DB* db = nullptr;
        Options ldb_options;
        ldb_options.create_if_missing = true;
        auto s = DB::Open(ldb_options, name, &db);
        if (!s.ok()) {
            throw std::runtime_error{ fmt::format("open db {} failed: {}", name, s.ToString()) };
        }

        db_.reset(db);
    }

    std::optional<std::string> query(std::string_view key) override
    {
        std::string r;
        auto s = db_->Get({}, as_slice(key), &r);
        if (s.ok()) {
            return r;
        } else if (s.IsNotFound()) {
            return {};
        } else {
            throw std::runtime_error{ fmt::format("query key {} failed: {}", key, s.ToString()) };
        }
    }

    void batch_add(const std::vector<key_value>& key_values) override
    {
        using namespace leveldb;

        WriteBatch batch;
        for (const auto& [k, v]: key_values) {
            batch.Put(as_slice(k), as_slice(v));
        }

        auto s = db_->Write({}, &batch);
        if (!s.ok()) {
            throw std::runtime_error{ fmt::format("write failed: {}", s.ToString()) };
        }
    }

    void remove(std::string_view key) override
    {
        auto s = db_->Delete({}, as_slice(key));
        if (!s.ok()) {
            throw std::runtime_error{ fmt::format("remove key {} failed: {}", key, s.ToString() )};
        }
    }

    std::map<std::string, std::string> query_range(
        std::string_view start_key,
        std::string_view end_key,
        int count) override
    {
        (void)start_key;
        (void)end_key;
        (void)count;
        return {};
    }

private:
    leveldb::Slice as_slice(std::string_view s) const
    {
        return leveldb::Slice(s.data(), s.size());
    }

private:
    std::unique_ptr<leveldb::DB> db_;
};

}