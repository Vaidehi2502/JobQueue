// Demonstration CLI for the persistent job queue.
//
// Registers a few example handlers, submits a mix of jobs (including a delayed
// one and a flaky one that exercises retries), runs a small worker pool until
// the queue drains, and prints a status summary. Job state persists to the
// SQLite file passed as the first argument, so re-running shows prior jobs and
// any that were left behind are recovered on startup.
//
// Usage: job_queue [db_path] [num_workers]

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>

#include "job_queue.hpp"

using namespace jobq;

int main(int argc, char** argv) {
    const std::string db_path = argc > 1 ? argv[1] : "jobs.db";
    const unsigned workers =
        argc > 2 ? static_cast<unsigned>(std::stoul(argv[2])) : 4;

    RetryPolicy retry;
    retry.max_attempts = 3;
    retry.initial_delay = std::chrono::milliseconds(100);
    retry.max_delay = std::chrono::milliseconds(2000);

    JobQueue queue(db_path, retry);

    // A plain handler that always succeeds.
    queue.register_handler("print", [](const Job& j) {
        std::cout << "[print] job " << j.id << ": " << j.payload << '\n';
        return JobResult::success();
    });

    // A flaky handler: fails the first two attempts, then succeeds. Shows the
    // retry-with-backoff path. attempts is 1-based and already counts the
    // current try.
    queue.register_handler("flaky", [](const Job& j) {
        if (j.attempts < 3) {
            return JobResult::retry("transient failure on attempt " +
                                    std::to_string(j.attempts));
        }
        std::cout << "[flaky] job " << j.id << " succeeded on attempt "
                  << j.attempts << '\n';
        return JobResult::success();
    });

    // A handler that always fails permanently.
    queue.register_handler("doomed", [](const Job&) {
        return JobResult::fail("this job can never succeed");
    });

    const int64_t recovered = queue.store().counts().queued;
    if (recovered > 0) {
        std::cout << "Starting with " << recovered
                  << " job(s) already queued from a previous run.\n";
    }

    queue.start(workers);

    for (int i = 0; i < 10; ++i) {
        queue.submit("print", "hello #" + std::to_string(i));
    }
    queue.submit("flaky", "needs a couple of tries");
    queue.submit("doomed", "will end up failed");

    // A delayed job: eligible only after 500ms.
    SubmitOptions delayed;
    delayed.max_attempts = 1;
    delayed.delay = std::chrono::milliseconds(500);
    queue.submit("print", "delayed greeting", delayed);

    std::cout << "Submitted jobs; waiting for the queue to drain...\n";
    if (!queue.drain(std::chrono::seconds(30))) {
        std::cout << "Timed out before the queue drained.\n";
    }
    queue.shutdown();

    const auto c = queue.counts();
    const auto m = queue.metrics();
    std::cout << "\n=== Summary ===\n"
              << "queued:    " << c.queued << '\n'
              << "running:   " << c.running << '\n'
              << "completed: " << c.completed << '\n'
              << "failed:    " << c.failed << '\n'
              << "--- metrics ---\n"
              << "submitted: " << m.submitted << '\n'
              << "processed: " << m.processed << '\n'
              << "retried:   " << m.retried << '\n'
              << "latency p50/p95/p99 (ms): " << m.latency_p50_ms << " / "
              << m.latency_p95_ms << " / " << m.latency_p99_ms << '\n';
    return 0;
}
