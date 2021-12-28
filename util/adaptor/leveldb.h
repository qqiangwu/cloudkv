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

    void batch_add(const write_batch& key_values) override
    {
        using namespace leveldb;

        struct handler : public write_batch::handler {
            WriteBatch batch;

            void add(std::string_view key, std::string_view val) override
            {
                batch.Put(as_slice(key), as_slice(val));
            }

            void remove(std::string_view key) override
            {
                batch.Delete(as_slice(key));
            }
        };

        handler h;
        key_values.iterate(&h);

        auto s = db_->Write({}, &h.batch);
        if (!s.ok()) {
            throw std::runtime_error{ fmt::format("write failed: {}", s.ToString()) };
        }
    }

    void add(std::string_view key, std::string_view value) override
    {
        auto s = db_->Put({}, as_slice(key), as_slice(value));
        if (!s.ok()) {
            throw std::runtime_error{ fmt::format("put kv failed: {}", s.ToString()) };
        }
    }

    void remove(std::string_view key) override
    {
        auto s = db_->Delete({}, as_slice(key));
        if (!s.ok()) {
            throw std::runtime_error{ fmt::format("remove key {} failed: {}", key, s.ToString() )};
        }
    }

    iter_ptr iter() override
    {
        // todo
        return {};
    }

private:
    static leveldb::Slice as_slice(std::string_view s)
    {
        return leveldb::Slice(s.data(), s.size());
    }

private:
    std::unique_ptr<leveldb::DB> db_;
};

}