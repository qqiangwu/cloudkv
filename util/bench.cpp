#include <atomic>
#include <thread>
#include <string>
#include <chrono>
#include <vector>
#include <functional>
#include <filesystem>
#include <memory>
#include <fmt/core.h>
#include <boost/thread/barrier.hpp>
#include <range/v3/view.hpp>
#include <gflags/gflags.h>
#include "cloudkv/db.h"

using namespace cloudkv;

using ranges::views::indices;

namespace fs = std::filesystem;

DEFINE_uint32(key_count, 1024 * 1024, "key count");
DEFINE_uint32(batch_size, 8, "batch per write");
DEFINE_uint32(thread_cnt, 4, "threads for benchmark");
DEFINE_uint32(key_size, 16, "key size");
DEFINE_uint32(val_size, 128, "value size");
DEFINE_string(db_name, "db_bench", "db name for bench");
DEFINE_bool(use_existing_db, false, "use existing db or create new");

struct Test_conf {
    boost::barrier started;
    boost::barrier stopped;
    kv_ptr db;
};

struct Thread_ctx {
    Test_conf& conf;
    int idx;
    std::atomic_uint64_t cnt;

    Thread_ctx(Test_conf& c, int i)
        : conf(c), idx(i), cnt(0)
    {}
};

void run_readrandom(Thread_ctx& ctx)
{
    const kv_ptr& kv = ctx.conf.db;

    while (true) {
        for (const auto i: indices(FLAGS_key_count)) {
            auto key = fmt::format("key-{:016}", i);
            kv->query(key);
            ctx.cnt.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void bench_thread(Thread_ctx& ctx)
{
    ctx.conf.started.count_down_and_wait();
    fmt::print("thread {} started\n", ctx.idx);

    run_readrandom(ctx);

    ctx.conf.stopped.count_down_and_wait();
}

void fill_db(const Test_conf& conf)
{
    struct raw_key_value {
        std::string key;
        std::string val;
    };
    std::vector<raw_key_value> raw;
    std::vector<key_value> batch;
    raw.reserve(FLAGS_batch_size);
    batch.reserve(FLAGS_batch_size);

    fmt::print("fill db with {} keys\n", FLAGS_key_count);
    for (const auto i: indices(FLAGS_key_count)) {
        (void)i;

        raw.clear();
        batch.clear();

        for (int k = 0; k < FLAGS_batch_size; ++k) {
            raw.push_back({
                fmt::format("key-{:016}", std::rand() % FLAGS_key_count),
                std::string(FLAGS_val_size, ' ')
            });
            batch.push_back({
                raw.back().key,
                raw.back().val
            });
        }

        conf.db->batch_add(batch);
    }
}

int main(int argc, char** argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (!FLAGS_use_existing_db) {
        fs::remove_all(FLAGS_db_name);
    }

    Test_conf conf {
        boost::barrier(FLAGS_thread_cnt + 1),
        boost::barrier(FLAGS_thread_cnt + 1),
        open(FLAGS_db_name, {})
    };

    if (!FLAGS_use_existing_db) {
        fill_db(conf);
    }

    std::vector<std::unique_ptr<Thread_ctx>> ctxs;
    for (const auto i: indices(FLAGS_thread_cnt)) {
        ctxs.push_back(std::make_unique<Thread_ctx>(conf, i));
        std::thread(bench_thread, std::ref(*ctxs.back())).detach();
    }

    fmt::print("{} threads launched, wait all started\n", FLAGS_thread_cnt);
    conf.started.count_down_and_wait();

    using namespace std::chrono_literals;
    while (true) {
        std::this_thread::sleep_for(1s);

        std::uint64_t cnts = 0;
        for (const auto& ctx: ctxs) {
            cnts += ctx->cnt.exchange(0, std::memory_order_relaxed);
        }

        fmt::print("qps={} latency={}ms\n", cnts, FLAGS_thread_cnt / float(cnts) * 1000);
    }
}