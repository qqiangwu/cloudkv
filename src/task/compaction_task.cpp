#include <cstddef>
#include <cassert>
#include <chrono>
#include <fstream>
#include <queue>
#include <range/v3/view.hpp>
#include <range/v3/to_container.hpp>
#include <spdlog/spdlog.h>
#include "task/compaction_task.h"
#include "sstable/sstable.h"
#include "sstable/sstable_builder.h"
#include "util/fmt_std.h"
#include "cloudkv/exception.h"

using namespace cloudkv;

namespace {

struct compaction_source {
    using iterator = std::vector<internal_key_value>::const_iterator;

    iterator beg;
    iterator end;
    int order;

    user_key_ref key() const
    {
        assert(beg != end);
        return beg->key.user_key();
    }

    bool empty() const
    {
        return beg == end;
    }
};

struct compaction_source_cmp {
    bool operator()(compaction_source& x, compaction_source& y) const
    {
        assert(x.beg != x.end);
        assert(y.beg != y.end);

        if (x.beg->key.user_key() == y.beg->key.user_key()) {
            return x.order < y.order;
        }

        return x.beg->key.user_key() > y.beg->key.user_key();
    }
};

auto make_compaction_sources(const std::vector<std::vector<internal_key_value>>& sources)
{
    std::priority_queue<compaction_source, std::vector<compaction_source>, compaction_source_cmp> 
        pq;
    
    for (size_t i = 0; i < sources.size(); ++i) {
        if (sources[i].empty()) {
            continue;
        }

        pq.push({
            sources[i].begin(),
            sources[i].end(),
            int(i)
        });
    }

    return pq;
}

}

void compaction_task::run()
{
    using namespace fmt;
    using namespace std::chrono;
    using namespace ranges;

    auto sstables_to_compaction = choose_files_();
    if (sstables_to_compaction.empty()) {
        spdlog::info("[compaction] no sstables choosed for compaction");
        notify_success_({}, {});
        return;
    }

    spdlog::info("[compaction] started, {} sst", sstables_.size());
    std::vector<std::vector<internal_key_value>> raw_sources = sstables_to_compaction
        | views::transform([](const auto& sst){
            return sst->query_range(sst->min(), sst->max());
        })
        | to<std::vector>;
    auto sources = make_compaction_sources(raw_sources);
    sstable_vec new_sstables;

    std::optional<std::string> last_key;
    std::optional<sstable_builder> builder;
    // todo: the algorithm
    while (!sources.empty()) {
        if (!builder) {
            builder.emplace(db_path_.next_sst_path());
        }

        auto src = sources.top();
        sources.pop();

        if (!last_key || last_key.value() != src.key()) {
            assert(!last_key || last_key.value() < src.key());

            const auto& kv = *src.beg; 
            builder->add(kv.key, kv.value);
            if (builder->size_in_bytes() > opts_.sstable_size) {
                builder->done();
                new_sstables.push_back(std::make_shared<sstable>(builder->target()));
                builder.reset();
            }

            last_key.emplace(src.key());
        }   

        ++src.beg;
        if (!src.empty()) {
            assert(src.key() > last_key.value());
            sources.push(src);
        }
    }

    if (builder && builder->size_in_bytes() > 0) {
        builder->done();
        new_sstables.push_back(std::make_shared<sstable>(builder->target()));
    }

    spdlog::info("[compaction] completed, {} sst generated", new_sstables.size());
    notify_success_(new_sstables, sstables_to_compaction);
}

compaction_task::sstable_vec compaction_task::choose_files_()
{
    sstable_vec compaction_vec;

    // todo: refine this, now compaction all
    return sstables_;
}