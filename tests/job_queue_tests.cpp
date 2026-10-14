// Functional tests: submission, lifecycle transitions, retry limits, delayed
// scheduling, invalid payloads, and handler failures.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "job_queue.hpp"

using namespace jobq;
using namespace std::chrono_literals;

namespace {
RetryPolicy fast_retry(int max_attempts = 3) {
    RetryPolicy p;
    p.max_attempts = max_attempts;
    p.initial_delay = 10ms;
    p.max_delay = 100ms;
    return p;
}
}  // namespace

TEST(Lifecycle, SubmitAndComplete) {
    JobQueue q(":memory:");
    std::atomic<int> ran{0};
    q.register_handler("ok", [&](const Job&) {
        ran.fetch_add(1);
        return JobResult::success();
    });
    q.start(2);

    int64_t id = q.submit("ok", "payload");
    EXPECT_GT(id, 0);

    ASSERT_TRUE(q.drain(5s));
    q.shutdown();

    EXPECT_EQ(ran.load(), 1);
    auto job = q.status(id);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->state, JobState::Completed);
    EXPECT_EQ(job->attempts, 1);
    EXPECT_EQ(q.counts().completed, 1);
}

TEST(Lifecycle, DurableIdBeforeExecution) {
    // submit() must persist the job before returning an id, even before any
    // worker runs.
    JobQueue q(":memory:");
    q.register_handler("ok", [](const Job&) { return JobResult::success(); });
    int64_t id = q.submit("ok", "data");
    auto job = q.status(id);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->state, JobState::Queued);
    EXPECT_EQ(job->payload, "data");
}

TEST(Retry, FlakyJobSucceedsAfterRetries) {
    JobQueue q(":memory:", fast_retry(3));
    std::atomic<int> attempts{0};
    q.register_handler("flaky", [&](const Job& j) {
        attempts.fetch_add(1);
        if (j.attempts < 3) return JobResult::retry("transient");
        return JobResult::success();
    });
    q.start(2);

    int64_t id = q.submit("flaky", "x");
    ASSERT_TRUE(q.drain(5s));
    q.shutdown();

    auto job = q.status(id);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->state, JobState::Completed);
    EXPECT_EQ(job->attempts, 3);
    EXPECT_EQ(attempts.load(), 3);
    EXPECT_EQ(q.metrics().retried, 2);
    EXPECT_EQ(q.counts().completed, 1);
}

TEST(Retry, ExhaustedRetriesMarkFailed) {
    JobQueue q(":memory:", fast_retry(2));
    std::atomic<int> attempts{0};
    q.register_handler("always_retry", [&](const Job&) {
        attempts.fetch_add(1);
        return JobResult::retry("still failing");
    });
    q.start(2);

    int64_t id = q.submit("always_retry", "x");
    ASSERT_TRUE(q.drain(5s));
    q.shutdown();

    auto job = q.status(id);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->state, JobState::Failed);
    EXPECT_EQ(job->attempts, 2);  // max_attempts
    EXPECT_EQ(attempts.load(), 2);
    EXPECT_EQ(q.metrics().retried, 1);   // one reschedule between the 2 attempts
    EXPECT_EQ(q.counts().failed, 1);
    EXPECT_NE(job->last_error.find("retries exhausted"), std::string::npos);
}

TEST(Failure, PermanentFailureNotRetried) {
    JobQueue q(":memory:", fast_retry(5));
    std::atomic<int> attempts{0};
    q.register_handler("doomed", [&](const Job&) {
        attempts.fetch_add(1);
        return JobResult::fail("nope");
    });
    q.start(2);

    int64_t id = q.submit("doomed", "x");
    ASSERT_TRUE(q.drain(5s));
    q.shutdown();

    auto job = q.status(id);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->state, JobState::Failed);
    EXPECT_EQ(attempts.load(), 1);  // no retries despite max_attempts=5
    EXPECT_EQ(job->last_error, "nope");
}

TEST(Failure, UnknownTypeFailsPermanently) {
    JobQueue q(":memory:");
    q.start(1);
    int64_t id = q.submit("no_such_handler", "x");
    ASSERT_TRUE(q.drain(5s));
    q.shutdown();

    auto job = q.status(id);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->state, JobState::Failed);
    EXPECT_NE(job->last_error.find("no handler"), std::string::npos);
}

TEST(Failure, HandlerExceptionIsRetried) {
    JobQueue q(":memory:", fast_retry(2));
    std::atomic<int> attempts{0};
    q.register_handler("throws", [&](const Job&) -> JobResult {
        attempts.fetch_add(1);
        throw std::runtime_error("boom");
    });
    q.start(2);
    int64_t id = q.submit("throws", "x");
    ASSERT_TRUE(q.drain(5s));
    q.shutdown();

    auto job = q.status(id);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->state, JobState::Failed);  // retried then exhausted
    EXPECT_EQ(attempts.load(), 2);
    EXPECT_NE(job->last_error.find("boom"), std::string::npos);
}

TEST(Scheduling, DelayedJobNotEligibleEarly) {
    JobQueue q(":memory:");
    std::atomic<int> ran{0};
    q.register_handler("later", [&](const Job&) {
        ran.fetch_add(1);
        return JobResult::success();
    });
    q.start(2);

    SubmitOptions opts;
    opts.max_attempts = 1;
    opts.delay = 300ms;
    int64_t id = q.submit("later", "x", opts);

    // Before it becomes eligible, it must still be queued and not have run.
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(ran.load(), 0);
    auto early = q.status(id);
    ASSERT_TRUE(early.has_value());
    EXPECT_EQ(early->state, JobState::Queued);

    ASSERT_TRUE(q.drain(5s));
    q.shutdown();
    EXPECT_EQ(ran.load(), 1);
    EXPECT_EQ(q.status(id)->state, JobState::Completed);
}

TEST(Backoff, ExponentialWithCap) {
    RetryPolicy p;
    p.initial_delay = 100ms;
    p.max_delay = 1000ms;
    EXPECT_EQ(p.backoff_for_retry(1), 100ms);
    EXPECT_EQ(p.backoff_for_retry(2), 200ms);
    EXPECT_EQ(p.backoff_for_retry(3), 400ms);
    EXPECT_EQ(p.backoff_for_retry(4), 800ms);
    EXPECT_EQ(p.backoff_for_retry(5), 1000ms);   // capped
    EXPECT_EQ(p.backoff_for_retry(50), 1000ms);  // no overflow
}
