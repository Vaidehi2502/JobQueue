#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "job.hpp"
#include "job_store.hpp"

namespace jobq {

// A snapshot of runtime counters. Values are monotonic since start() except
// where noted; take a copy and diff two snapshots to measure a window.
struct Metrics {
    int64_t submitted = 0;
    int64_t processed = 0;   // handler invocations that returned
    int64_t completed = 0;
    int64_t failed = 0;      // permanent failures (incl. retries exhausted)
    int64_t retried = 0;     // retryable failures rescheduled

    // Queue latency = time from a job becoming eligible (available_at) to a
    // worker claiming it. Reported in milliseconds.
    double latency_p50_ms = 0;
    double latency_p95_ms = 0;
    double latency_p99_ms = 0;
    int64_t latency_samples = 0;
};

// Persistent, multithreaded job queue. Jobs are submitted with a type and an
// opaque payload; a registered handler for that type executes each job on a
// worker thread. State is persisted to `store`, which survives restarts.
//
// Usage:
//   JobQueue q("jobs.db");
//   q.register_handler("email", [](const Job& j){ ...; return JobResult::success(); });
//   q.start(4);
//   q.submit("email", R"({"to":"a@b.com"})");
//   ...
//   q.drain();     // optional: wait until no work remains
//   q.shutdown();  // graceful stop + join
class JobQueue {
public:
    // `db_path` is passed through to JobStore (":memory:" for ephemeral).
    explicit JobQueue(const std::string& db_path,
                      RetryPolicy retry = RetryPolicy{});
    ~JobQueue();

    JobQueue(const JobQueue&) = delete;
    JobQueue& operator=(const JobQueue&) = delete;

    // Handlers must be registered before start() (and before the first job of
    // that type runs). Replacing a handler for an existing type is allowed.
    void register_handler(const std::string& type, JobHandler handler);

    // Runs crash recovery, then spawns `num_workers` threads. Idempotent guard:
    // calling start() twice throws.
    void start(unsigned num_workers);

    // Submits a job. Metadata is committed to the store before returning, so a
    // returned id is durably accepted. Returns the new job id. Overload lets
    // you override max_attempts / initial delay per submission.
    int64_t submit(const std::string& type, const std::string& payload = {});
    int64_t submit(const std::string& type, const std::string& payload,
                   const SubmitOptions& opts);

    // Blocks until there is no Queued or Running work left, or `timeout`
    // elapses. Returns true if the queue drained, false on timeout. Useful in
    // tests and batch runs; not required for normal operation.
    bool drain(std::chrono::milliseconds timeout = std::chrono::milliseconds::max());

    // Signals workers to stop and joins them. Jobs currently executing are
    // allowed to finish; Queued jobs are left in the store for next startup.
    // Safe to call multiple times and from the destructor.
    void shutdown();

    std::optional<Job> status(int64_t id);
    JobStore::Counts counts();
    Metrics metrics();

    JobStore& store() { return *store_; }

private:
    void worker_loop(unsigned worker_id);
    void handle_result(const Job& job, const JobResult& result);
    void record_latency_ms(double ms);
    void notify_work_available();
    JobHandler find_handler(const std::string& type);

    std::unique_ptr<JobStore> store_;
    RetryPolicy retry_;

    std::mutex handlers_mutex_;
    std::unordered_map<std::string, JobHandler> handlers_;

    // Coordinates worker sleeping/waking. See the .cpp for the lock-ordering
    // argument that makes this race-free against submit()/schedule_retry().
    std::mutex wait_mutex_;
    std::condition_variable work_cv_;
    std::atomic<bool> stopping_{false};
    std::atomic<bool> started_{false};
    std::vector<std::thread> workers_;

    // Metrics counters.
    std::atomic<int64_t> submitted_{0};
    std::atomic<int64_t> processed_{0};
    std::atomic<int64_t> completed_{0};
    std::atomic<int64_t> failed_{0};
    std::atomic<int64_t> retried_{0};
    std::atomic<int64_t> in_flight_{0};  // claimed but not yet resolved

    std::mutex latency_mutex_;
    std::vector<double> latency_samples_;
};

}  // namespace jobq
