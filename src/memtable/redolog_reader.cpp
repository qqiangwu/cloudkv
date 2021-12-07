#include <cassert>
#include "util/fmt_std.h"
#include "util/format.h"
#include "cloudkv/exception.h"
#include "memtable/redolog_reader.h"

using namespace cloudkv;

redolog_reader::redolog_reader(const path_t& p)
    : ifs_(p, std::ios::binary)
{
    if (!ifs_) {
        throw_system_error(fmt::format("open redolog {} failed", p));
    }
}

// todo: whatif redolog corrupted
std::optional<redo_entry> redolog_reader::next()
{
    assert(!ifs_.eof());

    std::uint32_t record_size;
    ifs_.read(reinterpret_cast<char*>(&record_size), sizeof(record_size));
    if (!ifs_) {
        return std::nullopt;
    }

    std::string buf(record_size, 0);
    ifs_.read(buf.data(), buf.size());
    if (!ifs_) {
        return std::nullopt;
    }

    const auto op = DecodeFixed32(buf.data());
    if (op > std::uint32_t(key_type::tombsome)) {
        throw data_corrupted{ "redolog corrupted" };
    }

    try {
        std::string key;
        std::string val;
        std::string_view sv = buf;
        sv.remove_prefix(sizeof(op));

        sv = decode_str(sv, &key);
        sv = decode_str(sv, &val);

        return redo_entry{ std::move(key), std::move(val), key_type(op) };
    } catch (std::invalid_argument&) {
        throw data_corrupted{ "redolog corrupted" };
    }
}