#pragma once

#include <vector>
#include <string_view>
#include <optional>
#include <map>
#include <memory>
#include <boost/noncopyable.hpp>
#include "cloudkv/options.h"
#include "cloudkv/iter.h"
#include "cloudkv/write_batch.h"

namespace cloudkv {

// all operations all atomic
class kv_store : private boost::noncopyable {
public:
    virtual ~kv_store() = default;

    virtual std::optional<std::string> query(std::string_view key) = 0;

    virtual void batch_add(const write_batch& batch) = 0;

    virtual void add(std::string_view key, std::string_view value) = 0;

    virtual void remove(std::string_view key) = 0;

    virtual iter_ptr iter() = 0;
};

using kv_ptr = std::unique_ptr<kv_store>;

// return nullptr if not exists
std::unique_ptr<kv_store> open(std::string_view name, const options& opts);

}
