#include <cassert>
#include "cloudkv/write_batch.h"
#include "write_batch_accessor.h"

using namespace cloudkv;

write_batch::write_batch()
    : buf_(std::string(write_batch_accessor::size_padding, 0))
{
}

void write_batch::add(std::string_view key, std::string_view value)
{
    write_batch_accessor(*this).add(key, value);
}

void write_batch::remove(std::string_view key)
{
    write_batch_accessor(*this).remove(key);
}

int write_batch::size() const
{
    return write_batch_accessor::size(*this);
}

void write_batch::iterate(handler* h) const
{
    write_batch_accessor::iterate(*this, [h](const auto op, const auto key, const auto val){
        switch (op) {
        case key_type::value:
            h->add(key, val);
            break;

        case key_type::tombsome:
            h->remove(key);
            break;

        default:
            assert(!"invalid operation");
            break;
        }
    });
}