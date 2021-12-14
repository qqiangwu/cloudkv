#include <cstddef>
#include <cassert>
#include <fstream>
#include <queue>
#include <spdlog/spdlog.h>
#include "task/compaction_task.h"
#include "sstable/sstable.h"
#include "sstable/sstable_builder.h"
#include "cloudkv/exception.h"

using namespace cloudkv;

namespace {

struct priority_stream {
    std::shared_ptr<kv_iter> it;
    internal_key_value kv;
    int priority;

    priority_stream(iter_ptr iter, int priority_)
        : it(std::move(iter)), kv(it->next()), priority(priority_)
    {
    }

    bool empty() const
    {
        return it->is_eof();
    }

    void advance()
    {
        kv = it->next();
    }

    std::string_view key() const
    {
        return kv.key.user_key();
    }

    const internal_key_value& key_value() const
    {
        return kv;
    }
};

struct stream_cmp {
    bool operator()(priority_stream& x, priority_stream& y) const
    {
        if (x.key() == y.key()) {
            return x.priority < y.priority;
        }

        return x.key() > y.key();
    }
};

auto to_priority_streams(const std::vector<sstable_ptr>& sstables)
{
    std::priority_queue<priority_stream, std::vector<priority_stream>, stream_cmp>
        pq;

    for (size_t i = 0; i < sstables.size(); ++i) {
        if (sstables[i]->count() == 0) {
            continue;
        }

        priority_stream stream { sstables[i]->iter(), int(i) };
        pq.push(std::move(stream));
    }

    return pq;
}

}

void compaction_task::run()
{
    auto sstables_to_compaction = choose_files_();
    if (sstables_to_compaction.empty()) {
        spdlog::info("[compaction] no sstables choosed for compaction");
        notify_success_({}, {});
        return;
    }

    spdlog::info("[compaction] started, {} sst", sstables_to_compaction.size());
    auto streams = to_priority_streams(sstables_to_compaction);
    sstable_vec new_sstables;

    std::optional<std::string> last_key;
    std::optional<sstable_builder> builder;
    // todo: the algorithm
    while (!streams.empty() && !is_cancelled()) {
        if (!builder) {
            builder.emplace(db_path_.sst_path(file_id_alloc_.alloc()));
        }

        auto stream = streams.top();
        streams.pop();

        if (!last_key || last_key.value() != stream.key()) {
            assert(!last_key || last_key.value() < stream.key());

            const auto& kv = stream.key_value();
            builder->add(kv.key, kv.value);
            if (builder->size_in_bytes() > opts_.sstable_size) {
                builder->done();
                new_sstables.push_back(std::make_shared<sstable>(builder->target()));
                builder.reset();
            }

            last_key.emplace(stream.key());
        }

        if (!stream.empty()) {
            stream.advance();
            assert(stream.key() > last_key.value());
            streams.push(stream);
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