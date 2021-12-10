#include <functional>
#include <range/v3/algorithm.hpp>
#include <gtest/gtest.h>
#include "batch_executor.h"

using namespace cloudkv;
using namespace ranges;

namespace {

struct Writer {
    write_batch batch;

    void operator()(const write_batch& b)
    {
        batch = b;
    }
};

}

TEST(batch_executor, NoBatch)
{
    Writer w;
    batch_executor executor(std::ref(w));

    write_batch batch;
    batch.add("123", "456");
    batch.remove("234");

    executor.submit(batch).get();

    ASSERT_EQ(w.batch.size(), batch.size());
    ASSERT_TRUE(equal(w.batch, batch, [](const auto& x, const auto& y){
        return x.op == y.op && x.key == y.key && x.value == y.value;
    }));
}

TEST(batch_executor, DoBatch)
{
    Writer w;
    batch_executor executor(std::ref(w));

    write_batch batch1;
    write_batch batch2;
    batch1.add("123", "456");
    batch2.add("234", "789");

    auto f1 = executor.submit(batch1);
    auto f2 = executor.submit(batch2);

    f1.get();
    f2.get();

    ASSERT_EQ(w.batch.size(), batch1.size() + batch2.size());
}

TEST(batch_executor, Exception)
{
    batch_executor executor([](const auto&){
        throw std::runtime_error{"fake error in batch commit"};
    });

    write_batch batch1;
    write_batch batch2;
    batch1.add("123", "456");
    batch2.add("234", "789");

    auto f1 = executor.submit(batch1);
    auto f2 = executor.submit(batch2);

    ASSERT_THROW(f1.get(), std::runtime_error);
    ASSERT_THROW(f2.get(), std::runtime_error);
}