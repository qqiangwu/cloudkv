#include <random>
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
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include "cloudkv/db.h"

using namespace cloudkv;

using ranges::views::indices;

namespace fs = std::filesystem;

DEFINE_string(benchmark, "readforever", "benchmark to perform");
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

using Benchmark_fn = void(*)(Thread_ctx&);

void fill_random(Thread_ctx& ctx)
{
    auto& db = ctx.conf.db;

    struct raw_key_value {
        std::string key;
        std::string val;
    };
    std::vector<raw_key_value> raw;
    std::vector<key_value> batch;
    raw.reserve(FLAGS_batch_size);
    batch.reserve(FLAGS_batch_size);

    std::random_device random_device;
    std::mt19937 engine{random_device()};
    std::uniform_int_distribution<int> dist(0, FLAGS_key_count - 1);

    for (const auto i: indices(FLAGS_key_count / FLAGS_batch_size)) {
        (void)i;

        raw.clear();
        batch.clear();

        for (const auto k: indices(FLAGS_batch_size)) {
            (void)k;

            raw.push_back({
                fmt::format("{:016}", dist(engine)),
                std::string(FLAGS_val_size, ' ')
            });
            batch.push_back({
                raw.back().key,
                raw.back().val
            });
        }

        db->batch_add(batch);
        ctx.cnt.fetch_add(FLAGS_batch_size, std::memory_order_relaxed);
    }
}

void read_random(Thread_ctx& ctx)
{
    std::random_device random_device;
    std::mt19937 engine{random_device()};
    std::uniform_int_distribution<int> dist(0, FLAGS_key_count - 1);

    const kv_ptr& kv = ctx.conf.db;
    for (const auto i: indices(FLAGS_key_count)) {
        (void)i;

        auto key = fmt::format("{:016}", dist(engine));
        kv->query(key);
        ctx.cnt.fetch_add(1, std::memory_order_relaxed);
    }
}

void read_random_forever(Thread_ctx& ctx)
{
    std::random_device random_device;
    std::mt19937 engine{random_device()};
    std::uniform_int_distribution<int> dist(0, FLAGS_key_count - 1);

    const kv_ptr& kv = ctx.conf.db;

    while (true) {
        for (const auto i: indices(FLAGS_key_count)) {
            (void)i;

            auto key = fmt::format("{:016}", dist(engine));
            kv->query(key);
            ctx.cnt.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void bench_thread(Thread_ctx& ctx, Benchmark_fn fn)
{
    ctx.conf.started.count_down_and_wait();
    fmt::print("thread {} started\n", ctx.idx);

    fn(ctx);

    ctx.conf.stopped.count_down_and_wait();
}

struct Bench_case {
    Benchmark_fn fn;
    const bool readonly;
};

Bench_case get_benchmark()
{
    if (FLAGS_benchmark == "fillrandom") {
        return { fill_random, false };
    } else if (FLAGS_benchmark == "readrandom") {
        return { read_random, true };
    } else if (FLAGS_benchmark == "readforever") {
        return { read_random_forever, true };
    } else {
        fmt::print("invalid benchmark {}, abort", FLAGS_benchmark);
        std::exit(1);
    }
}

int main(int argc, char** argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    auto new_logger = spdlog::basic_logger_mt("bench", "bench.log", true);
    spdlog::set_default_logger(new_logger);
    spdlog::flush_every(std::chrono::seconds(1));

    const auto bench_case = get_benchmark();

    if (!bench_case.readonly || !FLAGS_use_existing_db) {
        fmt::print("remove existing db {}\n", FLAGS_db_name);
        fs::remove_all(FLAGS_db_name);
    }

    std::atomic_bool stopped { false };
    Test_conf conf {
        boost::barrier(FLAGS_thread_cnt + 1),
        boost::barrier(FLAGS_thread_cnt, [&stopped]{ stopped = true; }),
        open(FLAGS_db_name, {})
    };

    std::vector<std::unique_ptr<Thread_ctx>> ctxs;
    for (const auto i: indices(FLAGS_thread_cnt)) {
        ctxs.push_back(std::make_unique<Thread_ctx>(conf, i));
        std::thread(bench_thread, std::ref(*ctxs.back()), bench_case.fn).detach();
    }

    fmt::print("run benchmark {}\n", FLAGS_benchmark);
    fmt::print("{} threads launched, wait all started\n", FLAGS_thread_cnt);
    conf.started.count_down_and_wait();

    using namespace std::chrono_literals;
    while (!stopped) {
        std::this_thread::sleep_for(1s);

        std::uint64_t cnts = 0;
        for (const auto& ctx: ctxs) {
            cnts += ctx->cnt.exchange(0, std::memory_order_relaxed);
        }

        fmt::print("qps={} latency={}ms\n", cnts, FLAGS_thread_cnt / float(cnts) * 1000);
    }
}
