#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "job.hpp"
#include "job_store.hpp"

namespace jobq {

// ---- Structured logging -----------------------------------------------------
enum class LogLevel { Debug, Info, Warn, Error };

const char* to_string(LogLevel level);

struct LogField {
    std::string key;
    std::string value;
};

// A logging sink. Default is none (zero cost). Install one with set_logger().
using Logger =
    std::function<void(LogLevel, std::string_view, const std::vector<LogField>&)>;

// Writes logfmt-style lines ("level=info msg=\"...\" key=value") to stderr at or
// above `min_level`. Thread-safe.
Logger make_stderr_logger(LogLevel min_level = LogLevel::Info);

// ---- Metrics ----------------------------------------------------------------
// A snapshot of runtime counters. Counters are monotonic since start(); gauges
// (pending, in_flight) are instantaneous.
struct Metrics {
    int64_t submitted = 0;
    int64_t processed = 0;   // handler invocations that returned
    int64_t completed = 0;
    int64_t failed = 0;      // permanent failures (incl. retries exhausted)
    int64_t retried = 0;     // retryable failures rescheduled
    int64_t timed_out = 0;   // jobs whose attempt exceeded its timeout
    int64_t rejected = 0;    // submissions refused because the queue was full

    int64_t pending = 0;     // gauge: queued + running
    int64_t in_flight = 0;   // gauge: handlers currently executing

    // Queue latency = time from a job becoming eligible (available_at) to a
    // worker claiming it, in milliseconds.
    double latency_p50_ms = 0;
    double latency_p95_ms = 0;
    double latency_p99_ms = 0;
    int64_t latency_samples = 0;
};

// Persistent, multithreaded job queue. Jobs are submitted with a type and an
// opaque payload; a registered handler for that type executes each job on a
// worker thread. State is persisted to `store`, which survives restarts.
//
// Optional capabilities (all off by default, configured before start()):
//   - bounded capacity + backpressure  (set_capacity)
//   - per-job timeouts via a watchdog   (SubmitOptions::timeout)
//   - structured logging                (set_logger)
//   - priorities                        (SubmitOptions::priority)
class JobQueue {
public:
    explicit JobQueue(const std::string& db_path,
                      RetryPolicy retry = RetryPolicy{});
    ~JobQueue();

    JobQueue(const JobQueue&) = delete;
    JobQueue& operator=(const JobQueue&) = delete;

    // ---- Configuration (call before start()) --------------------------------
    // Max pending (queued + running) jobs. 0 = unbounded. When set, submit()
    // blocks until there is room (backpressure) and try_submit() refuses.
    void set_capacity(size_t capacity);
    // How often the timeout watchdog scans for over-deadline running jobs.
    void set_watchdog_interval(std::chrono::milliseconds interval);
    void set_logger(Logger logger);

    // Handlers must be registered before the first job of that type runs.
    void register_handler(const std::string& type, JobHandler handler);

    // Runs crash recovery, then spawns `num_workers` worker threads plus a
    // watchdog thread. Throws if called twice.
    void start(unsigned num_workers);

    // ---- Submission ---------------------------------------------------------
    // Submits a job, blocking if the queue is at capacity until room is freed
    // or the queue shuts down. Returns the new job id. Metadata is committed to
    // the store before returning, so a returned id is durably accepted. Throws
    // std::runtime_error if the queue is shutting down while blocked.
    int64_t submit(const std::string& type, const std::string& payload = {});
    int64_t submit(const std::string& type, const std::string& payload,
                   const SubmitOptions& opts);

    // Non-blocking submit. Returns the id, or nullopt if the queue is at
    // capacity (the job is not enqueued).
    std::optional<int64_t> try_submit(const std::string& type,
                                      const std::string& payload,
                                      const SubmitOptions& opts);

    // Blocks until there is no Queued or Running work left, or `timeout`
    // elapses. Returns true if the queue drained, false on timeout.
    bool drain(std::chrono::milliseconds timeout = std::chrono::milliseconds::max());

    // Signals workers + watchdog to stop and joins them. Jobs currently
    // executing finish; Queued jobs are left in the store. Idempotent.
    void shutdown();

    // ---- Introspection ------------------------------------------------------
    std::optional<Job> status(int64_t id);
    std::vector<Job> list(std::optional<JobState> filter = std::nullopt,
                          int limit = 1000);
    JobStore::Counts counts();
    Metrics metrics();

    // Metrics rendered for operational tooling.
    std::string metrics_prometheus();
    std::string metrics_json();

    JobStore& store() { return *store_; }

private:
    enum class Resolution { Completed, Failed, Retried, Superseded };

    void worker_loop(unsigned worker_id);
    void watchdog_loop();
    Resolution handle_result(const Job& job, const JobResult& result);
    void on_terminal();  // one pending-slot freed
    void record_latency_ms(double ms);
    void notify_work_available();
    JobHandler find_handler(const std::string& type);
    void log(LogLevel level, std::string_view msg,
             std::vector<LogField> fields = {});

    std::unique_ptr<JobStore> store_;
    RetryPolicy retry_;

    std::mutex handlers_mutex_;
    std::unordered_map<std::string, JobHandler> handlers_;

    Logger logger_;  // empty = no logging

    // Worker sleep/wake coordination. See the .cpp for the lock-ordering
    // argument that makes this race-free against submit()/schedule_retry().
    std::mutex wait_mutex_;
    std::condition_variable work_cv_;
    std::atomic<bool> stopping_{false};
    std::atomic<bool> started_{false};
    std::vector<std::thread> workers_;
    std::thread watchdog_;
    std::chrono::milliseconds watchdog_interval_{100};

    // Bounded-capacity backpressure.
    std::mutex capacity_mutex_;
    std::condition_variable space_cv_;
    size_t capacity_ = 0;  // 0 = unbounded
    std::atomic<int64_t> pending_{0};  // queued + running (non-terminal)

    // Metrics counters.
    std::atomic<int64_t> submitted_{0};
    std::atomic<int64_t> processed_{0};
    std::atomic<int64_t> completed_{0};
    std::atomic<int64_t> failed_{0};
    std::atomic<int64_t> retried_{0};
    std::atomic<int64_t> timed_out_{0};
    std::atomic<int64_t> rejected_{0};
    std::atomic<int64_t> in_flight_{0};

    std::mutex latency_mutex_;
    std::vector<double> latency_samples_;
};

}  // namespace jobq
