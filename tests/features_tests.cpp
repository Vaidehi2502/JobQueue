// Tests for priorities, per-job timeouts, bounded capacity / backpressure, and
// metrics export.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

#include "job_queue.hpp"
#include "job_store.hpp"

using namespace jobq;
using namespace std::chrono_literals;

namespace {
// A one-shot gate handlers can block on until the test releases them.
struct Latch {
    std::mutex m;
    std::condition_variable cv;
    bool open = false;
    void wait() {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&] { return open; });
    }
    void release() {
        {
            std::lock_guard<std::mutex> lk(m);
            open = true;
        }
        cv.notify_all();
    }
};
}  // namespace

TEST(Priority, HigherPriorityClaimedFirst) {
    // Store-level, deterministic: claims come back in priority order regardless
    // of insertion order.
    JobStore s(":memory:");
    SubmitOptions lo;  lo.priority = 0;
    SubmitOptions mid; mid.priority = 5;
    SubmitOptions hi;  hi.priority = 10;

    s.insert("t", "lo", lo);
    s.insert("t", "hi", hi);
    s.insert("t", "mid", mid);

    const int64_t now = now_ms() + 1;
    auto a = s.claim_next(now);
    auto b = s.claim_next(now);
    auto c = s.claim_next(now);
    ASSERT_TRUE(a && b && c);
    EXPECT_EQ(a->payload, "hi");
    EXPECT_EQ(b->payload, "mid");
    EXPECT_EQ(c->payload, "lo");
    EXPECT_FALSE(s.claim_next(now).has_value());
}

TEST(Timeout, SingleAttemptTimesOutAndFails) {
    JobQueue q(":memory:");
    q.set_watchdog_interval(20ms);
    std::atomic<int> runs{0};
    q.register_handler("slow", [&](const Job&) {
        runs.fetch_add(1);
        std::this_thread::sleep_for(300ms);  // exceeds the 50ms budget
        return JobResult::success();
    });
    q.start(2);

    SubmitOptions o;
    o.max_attempts = 1;
    o.timeout = 50ms;
    int64_t id = q.submit("slow", "x", o);

    ASSERT_TRUE(q.drain(5s));
    q.shutdown();

    auto job = q.status(id);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->state, JobState::Failed);
    EXPECT_GE(q.metrics().timed_out, 1);
    // The handler ran, but its success result was discarded as superseded.
    EXPECT_EQ(runs.load(), 1);
}

TEST(Timeout, RetriesAfterTimeoutThenExhausts) {
    RetryPolicy retry;
    retry.initial_delay = 20ms;
    retry.max_delay = 50ms;
    JobQueue q(":memory:", retry);
    q.set_watchdog_interval(15ms);

    std::atomic<int> runs{0};
    q.register_handler("slow", [&](const Job&) {
        runs.fetch_add(1);
        std::this_thread::sleep_for(120ms);  // always exceeds 40ms budget
        return JobResult::success();
    });
    q.start(2);

    SubmitOptions o;
    o.max_attempts = 2;
    o.timeout = 40ms;
    int64_t id = q.submit("slow", "x", o);

    ASSERT_TRUE(q.drain(10s));
    q.shutdown();

    auto job = q.status(id);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->state, JobState::Failed);
    EXPECT_EQ(job->attempts, 2);
    EXPECT_GE(q.metrics().timed_out, 2);
    EXPECT_GE(q.metrics().retried, 1);
    EXPECT_EQ(runs.load(), 2);  // one run per attempt
}

TEST(Capacity, TrySubmitRejectedWhenFull) {
    JobQueue q(":memory:");
    q.set_capacity(3);
    Latch latch;
    q.register_handler("block", [&](const Job&) {
        latch.wait();
        return JobResult::success();
    });
    q.start(1);

    // Fill capacity: 1 running (blocked) + 2 queued == 3 pending.
    EXPECT_GT(q.submit("block", "a"), 0);
    EXPECT_GT(q.submit("block", "b"), 0);
    EXPECT_GT(q.submit("block", "c"), 0);

    SubmitOptions opts;
    auto rejected = q.try_submit("block", "d", opts);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_GE(q.metrics().rejected, 1);
    EXPECT_EQ(q.metrics().pending, 3);

    latch.release();
    ASSERT_TRUE(q.drain(5s));
    q.shutdown();
    EXPECT_EQ(q.counts().completed, 3);
}

TEST(Capacity, BlockingSubmitAppliesBackpressure) {
    JobQueue q(":memory:");
    q.set_capacity(2);
    std::atomic<int> done{0};
    q.register_handler("w", [&](const Job&) {
        std::this_thread::sleep_for(15ms);
        done.fetch_add(1);
        return JobResult::success();
    });
    q.start(2);

    constexpr int kTotal = 12;
    for (int i = 0; i < kTotal; ++i) {
        // submit() blocks once pending hits capacity and resumes as jobs drain.
        q.submit("w", std::to_string(i));
        EXPECT_LE(q.metrics().pending, 2);  // soft bound respected
    }

    ASSERT_TRUE(q.drain(10s));
    q.shutdown();
    EXPECT_EQ(done.load(), kTotal);
    EXPECT_EQ(q.counts().completed, kTotal);
}

TEST(MetricsExport, PrometheusAndJsonContainCounters) {
    JobQueue q(":memory:");
    q.register_handler("ok", [](const Job&) { return JobResult::success(); });
    q.start(2);
    for (int i = 0; i < 5; ++i) q.submit("ok", "x");
    ASSERT_TRUE(q.drain(5s));
    q.shutdown();

    const std::string prom = q.metrics_prometheus();
    EXPECT_NE(prom.find("jobq_completed_total 5"), std::string::npos);
    EXPECT_NE(prom.find("# TYPE jobq_submitted_total counter"),
              std::string::npos);
    EXPECT_NE(prom.find("jobq_queue_latency_ms{quantile=\"0.99\"}"),
              std::string::npos);

    const std::string json = q.metrics_json();
    EXPECT_NE(json.find("\"completed\":5"), std::string::npos);
    EXPECT_NE(json.find("\"submitted\":5"), std::string::npos);
}
