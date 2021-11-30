#pragma once

#include <optional>
#include "sstable/sstable.h"

namespace cloudkv {

/**
 * @brief simple format: sorted string list
 * 
 * [record1]: key_size|key_chars|seq|key_type|value_size|value_chars
 * [record2]
 * [record3]
 * ...
 * [recordN]
 * [footer]
 *      min_key: key_size(fix32)|key_chars
 *      max_key: key_size(fix32)|key_chars
 *      key_value_count: fix32
 * 
 */
class sstable_builder {
public:
    explicit sstable_builder(std::ostream& out);

    void add(const internal_key& key, std::string_view value);
    
    void done();

private:
    std::string build_record_(const internal_key& key, std::string_view value);
    std::string build_footer_();

private:
    std::ostream& out_;
    
    struct ctx {
        std::string key_min;
        std::string key_max;
        int count;
    };

    std::optional<ctx> ctx_;
};

}