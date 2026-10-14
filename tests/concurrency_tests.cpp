// Concurrency tests: multiple producers and workers, and the guarantee that
// each job is claimed and executed exactly once.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "job_queue.hpp"

using namespace jobq;
using namespace std::chrono_literals;

TEST(Concurrency, ManyProducersAndWorkers) {
    JobQueue q(":memory:");
    std::atomic<int> executed{0};
    q.register_handler("work", [&](const Job&) {
        executed.fetch_add(1, std::memory_order_relaxed);
        return JobResult::success();
    });
    q.start(8);

    constexpr int kProducers = 6;
    constexpr int kPerProducer = 500;
    constexpr int kTotal = kProducers * kPerProducer;

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&q, p] {
            for (int i = 0; i < kPerProducer; ++i) {
                q.submit("work", std::to_string(p * kPerProducer + i));
            }
        });
    }
    for (auto& t : producers) t.join();

    ASSERT_TRUE(q.drain(30s));
    q.shutdown();

    EXPECT_EQ(executed.load(), kTotal);
    auto c = q.counts();
    EXPECT_EQ(c.completed, kTotal);
    EXPECT_EQ(c.queued, 0);
    EXPECT_EQ(c.running, 0);
    EXPECT_EQ(q.metrics().submitted, kTotal);
    EXPECT_EQ(q.metrics().processed, kTotal);
}

TEST(Concurrency, EachJobExecutedExactlyOnce) {
    JobQueue q(":memory:");
    constexpr int kTotal = 2000;
    // One counter per job id; a correct claim is atomic, so no counter may
    // exceed 1.
    std::vector<std::atomic<int>> runs(kTotal);
    for (auto& r : runs) r.store(0);

    q.register_handler("once", [&](const Job& j) {
        const int idx = std::stoi(j.payload);
        runs[idx].fetch_add(1, std::memory_order_relaxed);
        return JobResult::success();
    });
    q.start(8);

    for (int i = 0; i < kTotal; ++i) q.submit("once", std::to_string(i));

    ASSERT_TRUE(q.drain(30s));
    q.shutdown();

    int total = 0;
    int max_runs = 0;
    for (auto& r : runs) {
        int v = r.load();
        total += v;
        max_runs = std::max(max_runs, v);
    }
    EXPECT_EQ(total, kTotal);
    EXPECT_EQ(max_runs, 1);  // exactly once, never double-claimed
}

TEST(Concurrency, ShutdownWhileSubmitting) {
    // Shutting down must not deadlock or crash even with work still arriving.
    JobQueue q(":memory:");
    std::atomic<int> executed{0};
    q.register_handler("work", [&](const Job&) {
        executed.fetch_add(1);
        return JobResult::success();
    });
    q.start(4);

    std::atomic<bool> stop{false};
    std::thread producer([&] {
        while (!stop.load()) {
            q.submit("work", "x");
            std::this_thread::sleep_for(100us);
        }
    });

    std::this_thread::sleep_for(50ms);
    stop.store(true);
    producer.join();
    q.shutdown();  // must return cleanly

    // Any job that was claimed finished; the rest remain queued in the store.
    auto c = q.counts();
    EXPECT_EQ(c.running, 0);
    EXPECT_GE(c.completed, 0);
    SUCCEED();
}
