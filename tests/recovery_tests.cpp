// Persistence and crash-recovery tests. These use an on-disk database so state
// survives across JobStore/JobQueue instances.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>

#include "job_queue.hpp"
#include "job_store.hpp"
#include "test_util.hpp"

using namespace jobq;
using namespace std::chrono_literals;

TEST(Persistence, JobsSurviveQueueRestart) {
    TempDb db;
    int64_t id = 0;
    {
        JobQueue q(db.path());
        q.register_handler("ok", [](const Job&) { return JobResult::success(); });
        q.start(2);
        id = q.submit("ok", "persisted");
        ASSERT_TRUE(q.drain(5s));
        q.shutdown();
        EXPECT_EQ(q.status(id)->state, JobState::Completed);
    }
    // Reopen the same database with a fresh queue.
    {
        JobQueue q(db.path());
        auto job = q.status(id);
        ASSERT_TRUE(job.has_value());
        EXPECT_EQ(job->state, JobState::Completed);
        EXPECT_EQ(job->payload, "persisted");
    }
}

TEST(Recovery, QueuedJobRunsAfterRestart) {
    TempDb db;
    int64_t id = 0;
    {
        // Submit but never start workers: the job stays queued and persisted.
        JobQueue q(db.path());
        id = q.submit("ok", "pending");
        EXPECT_EQ(q.status(id)->state, JobState::Queued);
    }
    {
        // A new process picks up the pending job and completes it.
        JobQueue q(db.path());
        std::atomic<int> ran{0};
        q.register_handler("ok", [&](const Job&) {
            ran.fetch_add(1);
            return JobResult::success();
        });
        q.start(2);
        ASSERT_TRUE(q.drain(5s));
        q.shutdown();
        EXPECT_EQ(ran.load(), 1);
        EXPECT_EQ(q.status(id)->state, JobState::Completed);
    }
}

TEST(Recovery, InterruptedRunningJobIsRequeued) {
    TempDb db;
    int64_t id = 0;
    {
        // Simulate a crash mid-execution: claim the job (-> running) and then
        // drop the store without resolving it.
        JobStore store(db.path());
        SubmitOptions opts;
        opts.max_attempts = 3;
        id = store.insert("ok", "interrupted", opts);
        auto claimed = store.claim_next(now_ms());
        ASSERT_TRUE(claimed.has_value());
        EXPECT_EQ(claimed->state, JobState::Running);
        EXPECT_EQ(store.counts().running, 1);
    }
    {
        // Recovery alone should move it back to queued.
        JobStore store(db.path());
        EXPECT_EQ(store.counts().running, 1);
        int recovered = store.recover_interrupted();
        EXPECT_EQ(recovered, 1);
        EXPECT_EQ(store.counts().queued, 1);
        EXPECT_EQ(store.counts().running, 0);
    }
    {
        // And a started queue (which runs recovery) should finish it.
        JobQueue q(db.path());
        std::atomic<int> ran{0};
        q.register_handler("ok", [&](const Job& j) {
            ran.fetch_add(1);
            // The attempt counter carried over from the interrupted run.
            EXPECT_GE(j.attempts, 2);
            return JobResult::success();
        });
        q.start(2);
        ASSERT_TRUE(q.drain(5s));
        q.shutdown();
        EXPECT_EQ(ran.load(), 1);
        EXPECT_EQ(q.status(id)->state, JobState::Completed);
    }
}

TEST(Recovery, StartRunsRecoveryForRunningJobs) {
    TempDb db;
    {
        JobStore store(db.path());
        SubmitOptions opts;
        store.insert("ok", "a", opts);
        store.insert("ok", "b", opts);
        store.claim_next(now_ms());
        store.claim_next(now_ms());
        EXPECT_EQ(store.counts().running, 2);
    }
    {
        JobQueue q(db.path());
        std::atomic<int> ran{0};
        q.register_handler("ok", [&](const Job&) {
            ran.fetch_add(1);
            return JobResult::success();
        });
        q.start(3);  // start() calls recover_interrupted()
        ASSERT_TRUE(q.drain(5s));
        q.shutdown();
        EXPECT_EQ(ran.load(), 2);
        EXPECT_EQ(q.counts().completed, 2);
    }
}
