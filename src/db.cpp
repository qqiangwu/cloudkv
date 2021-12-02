#include "cloudkv/db.h"
#include "db_impl.h"

using namespace cloudkv;

std::unique_ptr<kv_store> cloudkv::open(std::string_view name, const options& opts)
{
    return std::make_unique<db_impl>(name, opts);
}