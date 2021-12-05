#include <thread>
#include <string>
#include <chrono>
#include <vector>
#include <functional>
#include <memory>
#include <fmt/core.h>
#include <boost/thread/barrier.hpp>
#include "cloudkv/db.h"

using namespace cloudkv;

const int key_count = 1024 * 1024;
const int batch_size = 8;

struct Test_conf {
    const int thread_count;
    const std::uint64_t key_size;
    const std::uint64_t val_size;
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
        for (int i = 0; i < key_count; ++i) {
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
    raw.reserve(batch_size);
    batch.reserve(batch_size);

    fmt::print("fill db with {} keys", key_count);
    for (int i = 0; i < key_count; ++i) {
        raw.clear();
        batch.clear();

        for (int k = 0; k < batch_size; ++k) {
            raw.push_back({
                fmt::format("key-{:016}", std::rand() % key_count),
                std::string(conf.val_size, ' ')
            });
            batch.push_back({
                raw.back().key,
                raw.back().val
            });
        }

        conf.db->batch_add(batch);
    }
}

int main(int argc, const char** argv)
{
    const int thread_cnt = argc > 1? std::stoi(argv[1]): 4;
    const std::uint64_t key_size = argc > 2? std::stoull(argv[2]): 64;
    const std::uint64_t val_size = argc > 3? std::stoull(argv[3]): 512;

    Test_conf conf {
        thread_cnt,
        key_size,
        val_size,
        boost::barrier(thread_cnt + 1),
        boost::barrier(thread_cnt + 1),
        open("db_bench", {})
    };

    fill_db(conf);

    std::vector<std::unique_ptr<Thread_ctx>> ctxs;
    for (int i = 0; i < thread_cnt; ++i) {
        ctxs.push_back(std::make_unique<Thread_ctx>(conf, i));
        std::thread(bench_thread, std::ref(*ctxs.back())).detach();
    }

    fmt::print("{} threads launched, wait all started\n", thread_cnt);
    conf.started.count_down_and_wait();

    using namespace std::chrono_literals;
    while (true) {
        std::this_thread::sleep_for(1s);

        std::uint64_t cnts = 0;
        for (const auto& ctx: ctxs) {
            cnts += ctx->cnt.exchange(0, std::memory_order_relaxed);
        }

        fmt::print("qps={} latency={}ms\n", cnts, thread_cnt / float(cnts) * 1000);
    }
}