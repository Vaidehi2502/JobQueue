#include "job_queue.hpp"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>

namespace jobq {

const char* to_string(LogLevel level) {
    switch (level) {
        case LogLevel::Debug: return "debug";
        case LogLevel::Info: return "info";
        case LogLevel::Warn: return "warn";
        case LogLevel::Error: return "error";
    }
    return "info";
}

Logger make_stderr_logger(LogLevel min_level) {
    auto mu = std::make_shared<std::mutex>();
    return [mu, min_level](LogLevel level, std::string_view msg,
                           const std::vector<LogField>& fields) {
        if (static_cast<int>(level) < static_cast<int>(min_level)) return;
        std::ostringstream os;
        os << "level=" << to_string(level) << " msg=\"" << msg << '"';
        for (const auto& f : fields) os << ' ' << f.key << '=' << f.value;
        std::lock_guard<std::mutex> lock(*mu);
        std::cerr << os.str() << '\n';
    };
}

JobQueue::JobQueue(const std::string& db_path, RetryPolicy retry)
    : store_(std::make_unique<JobStore>(db_path)), retry_(retry) {}

JobQueue::~JobQueue() { shutdown(); }

void JobQueue::set_capacity(size_t capacity) {
    if (started_.load()) throw std::logic_error("set_capacity after start()");
    capacity_ = capacity;
}

void JobQueue::set_watchdog_interval(std::chrono::milliseconds interval) {
    if (started_.load()) throw std::logic_error("set_watchdog_interval after start()");
    if (interval.count() > 0) watchdog_interval_ = interval;
}

void JobQueue::set_logger(Logger logger) { logger_ = std::move(logger); }

void JobQueue::log(LogLevel level, std::string_view msg,
                   std::vector<LogField> fields) {
    if (logger_) logger_(level, msg, fields);
}

void JobQueue::register_handler(const std::string& type, JobHandler handler) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    handlers_[type] = std::move(handler);
}

JobHandler JobQueue::find_handler(const std::string& type) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    auto it = handlers_.find(type);
    return it == handlers_.end() ? JobHandler{} : it->second;
}

void JobQueue::start(unsigned num_workers) {
    if (started_.exchange(true)) {
        throw std::logic_error("JobQueue::start called more than once");
    }
    if (num_workers == 0) num_workers = 1;

    // Reconcile work left Running by a previous process before workers begin.
    const int recovered = store_->recover_interrupted();
    // Seed the pending gauge from whatever is already non-terminal on disk.
    pending_.store(store_->counts().pending());

    log(LogLevel::Info, "queue started",
        {{"workers", std::to_string(num_workers)},
         {"recovered", std::to_string(recovered)},
         {"capacity", std::to_string(capacity_)}});

    workers_.reserve(num_workers);
    for (unsigned i = 0; i < num_workers; ++i) {
        workers_.emplace_back([this, i] { worker_loop(i); });
    }
    watchdog_ = std::thread([this] { watchdog_loop(); });
}

// ---- Submission -------------------------------------------------------------

int64_t JobQueue::submit(const std::string& type, const std::string& payload) {
    SubmitOptions opts;
    opts.max_attempts = retry_.max_attempts;
    return submit(type, payload, opts);
}

int64_t JobQueue::submit(const std::string& type, const std::string& payload,
                         const SubmitOptions& opts) {
    // Reserve a pending slot, blocking if the queue is at capacity until a job
    // completes (backpressure).
    bool reserved;
    {
        std::unique_lock<std::mutex> lk(capacity_mutex_);
        if (capacity_ == 0) {
            reserved = true;
        } else {
            space_cv_.wait(lk, [&] {
                return stopping_.load() ||
                       pending_.load() < static_cast<int64_t>(capacity_);
            });
            reserved = !stopping_.load();
        }
        if (reserved) pending_.fetch_add(1, std::memory_order_relaxed);
    }
    if (!reserved) throw std::runtime_error("queue is shutting down");

    int64_t id;
    try {
        id = store_->insert(type, payload, opts);
    } catch (...) {
        on_terminal();  // give the reserved slot back
        throw;
    }
    submitted_.fetch_add(1, std::memory_order_relaxed);
    notify_work_available();
    return id;
}

std::optional<int64_t> JobQueue::try_submit(const std::string& type,
                                            const std::string& payload,
                                            const SubmitOptions& opts) {
    {
        std::unique_lock<std::mutex> lk(capacity_mutex_);
        if (capacity_ != 0 &&
            pending_.load() >= static_cast<int64_t>(capacity_)) {
            rejected_.fetch_add(1, std::memory_order_relaxed);
            log(LogLevel::Debug, "submission rejected (queue full)",
                {{"type", type}});
            return std::nullopt;
        }
        pending_.fetch_add(1, std::memory_order_relaxed);
    }
    int64_t id;
    try {
        id = store_->insert(type, payload, opts);
    } catch (...) {
        on_terminal();
        throw;
    }
    submitted_.fetch_add(1, std::memory_order_relaxed);
    notify_work_available();
    return id;
}

void JobQueue::notify_work_available() {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    work_cv_.notify_one();
}

void JobQueue::on_terminal() {
    pending_.fetch_sub(1, std::memory_order_release);
    if (capacity_ != 0) {
        std::lock_guard<std::mutex> lk(capacity_mutex_);
        space_cv_.notify_one();
    }
}

// ---- Worker loop ------------------------------------------------------------

void JobQueue::worker_loop([[maybe_unused]] unsigned worker_id) {
    while (true) {
        std::optional<Job> job;
        {
            std::unique_lock<std::mutex> lock(wait_mutex_);
            if (stopping_.load(std::memory_order_acquire)) break;

            job = store_->claim_next(now_ms());
            if (!job) {
                // No eligible work. Sleep until notified, or until the soonest
                // delayed job becomes eligible. Holding wait_mutex_ across
                // claim + wait closes the missed-wakeup window (a concurrent
                // submit()/schedule_retry() blocks on wait_mutex_ until we
                // release it by waiting); plain waits tolerate spurious ones.
                if (auto next = store_->next_available_at()) {
                    work_cv_.wait_until(lock, from_epoch_ms(*next));
                } else {
                    work_cv_.wait(lock);
                }
                continue;
            }
        }

        // Execute outside every lock so long-running handlers never block other
        // workers from claiming jobs.
        in_flight_.fetch_add(1, std::memory_order_relaxed);
        record_latency_ms(
            std::max<double>(0, static_cast<double>(now_ms() - job->available_at)));

        JobResult result;
        if (JobHandler handler = find_handler(job->type)) {
            try {
                result = handler(*job);
            } catch (const std::exception& e) {
                result = JobResult::retry(std::string("handler threw: ") + e.what());
            } catch (...) {
                result = JobResult::retry("handler threw: unknown exception");
            }
        } else {
            result = JobResult::fail("no handler registered for type '" +
                                     job->type + "'");
        }

        processed_.fetch_add(1, std::memory_order_relaxed);
        handle_result(*job, result);
        in_flight_.fetch_sub(1, std::memory_order_relaxed);
    }
}

// Applies a handler (or watchdog) result using the store's conditional
// resolution, so a result for a superseded attempt (e.g. one the watchdog timed
// out and another worker re-claimed) is discarded. Counters and the pending
// gauge only move when the store actually transitioned the row.
JobQueue::Resolution JobQueue::handle_result(const Job& job,
                                             const JobResult& result) {
    switch (result.outcome) {
        case JobOutcome::Success:
            if (store_->mark_completed(job.id, job.attempts)) {
                completed_.fetch_add(1, std::memory_order_relaxed);
                on_terminal();
                return Resolution::Completed;
            }
            return Resolution::Superseded;

        case JobOutcome::PermanentFailure:
            if (store_->mark_failed(job.id, result.message, job.attempts)) {
                failed_.fetch_add(1, std::memory_order_relaxed);
                on_terminal();
                log(LogLevel::Warn, "job failed permanently",
                    {{"id", std::to_string(job.id)},
                     {"type", job.type},
                     {"error", result.message}});
                return Resolution::Failed;
            }
            return Resolution::Superseded;

        case JobOutcome::RetryableFailure:
            // job.attempts already counts this attempt (claim incremented it).
            if (job.attempts >= job.max_attempts) {
                const std::string err =
                    result.message.empty()
                        ? "retries exhausted"
                        : result.message + " (retries exhausted)";
                if (store_->mark_failed(job.id, err, job.attempts)) {
                    failed_.fetch_add(1, std::memory_order_relaxed);
                    on_terminal();
                    log(LogLevel::Warn, "job failed (retries exhausted)",
                        {{"id", std::to_string(job.id)},
                         {"type", job.type},
                         {"attempts", std::to_string(job.attempts)}});
                    return Resolution::Failed;
                }
                return Resolution::Superseded;
            }
            {
                const auto delay = retry_.backoff_for_retry(job.attempts);
                if (store_->schedule_retry(job.id, now_ms() + delay.count(),
                                           result.message, job.attempts)) {
                    retried_.fetch_add(1, std::memory_order_relaxed);
                    notify_work_available();
                    log(LogLevel::Info, "job scheduled for retry",
                        {{"id", std::to_string(job.id)},
                         {"type", job.type},
                         {"attempt", std::to_string(job.attempts)},
                         {"delay_ms", std::to_string(delay.count())}});
                    return Resolution::Retried;
                }
                return Resolution::Superseded;
            }
    }
    return Resolution::Superseded;
}

// ---- Watchdog ---------------------------------------------------------------

void JobQueue::watchdog_loop() {
    using namespace std::chrono;
    while (!stopping_.load(std::memory_order_acquire)) {
        // Sleep in small slices so shutdown is prompt regardless of interval.
        const auto deadline = steady_clock::now() + watchdog_interval_;
        while (steady_clock::now() < deadline &&
               !stopping_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(
                std::min(milliseconds(20),
                         duration_cast<milliseconds>(deadline - steady_clock::now())));
        }
        if (stopping_.load(std::memory_order_acquire)) break;

        for (const auto& job : store_->find_timed_out(now_ms())) {
            // A timeout does NOT stop the in-flight handler (C++ threads cannot
            // be safely cancelled); it makes the job eligible again. Conditional
            // resolution ensures the stale handler's later result is ignored.
            const std::string msg =
                "timed out after " + std::to_string(job.timeout_ms) + "ms";
            const Resolution r = handle_result(job, JobResult::retry(msg));
            if (r != Resolution::Superseded) {
                timed_out_.fetch_add(1, std::memory_order_relaxed);
                log(LogLevel::Warn, "job timed out",
                    {{"id", std::to_string(job.id)},
                     {"type", job.type},
                     {"timeout_ms", std::to_string(job.timeout_ms)}});
            }
        }
    }
}

// ---- Lifecycle --------------------------------------------------------------

void JobQueue::record_latency_ms(double ms) {
    std::lock_guard<std::mutex> lock(latency_mutex_);
    latency_samples_.push_back(ms);
}

bool JobQueue::drain(std::chrono::milliseconds timeout) {
    const auto start = std::chrono::steady_clock::now();
    const bool bounded = timeout != std::chrono::milliseconds::max();
    for (;;) {
        const auto c = store_->counts();
        if (c.queued == 0 && c.running == 0 &&
            in_flight_.load(std::memory_order_acquire) == 0) {
            return true;
        }
        if (bounded && std::chrono::steady_clock::now() - start >= timeout) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void JobQueue::shutdown() {
    bool expected = false;
    if (!stopping_.compare_exchange_strong(expected, true)) {
        return;  // already shutting down
    }
    {
        std::lock_guard<std::mutex> lock(wait_mutex_);
        work_cv_.notify_all();
    }
    {
        std::lock_guard<std::mutex> lk(capacity_mutex_);
        space_cv_.notify_all();  // release any blocked submit()
    }
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
    if (watchdog_.joinable()) watchdog_.join();
    log(LogLevel::Info, "queue stopped",
        {{"completed", std::to_string(completed_.load())},
         {"failed", std::to_string(failed_.load())}});
}

// ---- Introspection ----------------------------------------------------------

std::optional<Job> JobQueue::status(int64_t id) { return store_->get(id); }

std::vector<Job> JobQueue::list(std::optional<JobState> filter, int limit) {
    return store_->list(filter, limit);
}

JobStore::Counts JobQueue::counts() { return store_->counts(); }

Metrics JobQueue::metrics() {
    Metrics m;
    m.submitted = submitted_.load();
    m.processed = processed_.load();
    m.completed = completed_.load();
    m.failed = failed_.load();
    m.retried = retried_.load();
    m.timed_out = timed_out_.load();
    m.rejected = rejected_.load();
    m.pending = pending_.load();
    m.in_flight = in_flight_.load();

    std::vector<double> samples;
    {
        std::lock_guard<std::mutex> lock(latency_mutex_);
        samples = latency_samples_;
    }
    m.latency_samples = static_cast<int64_t>(samples.size());
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        auto pct = [&](double p) {
            size_t idx =
                static_cast<size_t>(std::ceil(p / 100.0 * samples.size()));
            if (idx > 0) --idx;
            idx = std::min(idx, samples.size() - 1);
            return samples[idx];
        };
        m.latency_p50_ms = pct(50);
        m.latency_p95_ms = pct(95);
        m.latency_p99_ms = pct(99);
    }
    return m;
}

std::string JobQueue::metrics_prometheus() {
    const Metrics m = metrics();
    std::ostringstream os;
    auto counter = [&](const char* name, const char* help, int64_t v) {
        os << "# HELP " << name << ' ' << help << '\n'
           << "# TYPE " << name << " counter\n"
           << name << ' ' << v << '\n';
    };
    auto gauge = [&](const char* name, const char* help, double v) {
        os << "# HELP " << name << ' ' << help << '\n'
           << "# TYPE " << name << " gauge\n"
           << name << ' ' << v << '\n';
    };
    counter("jobq_submitted_total", "Jobs submitted", m.submitted);
    counter("jobq_processed_total", "Handler invocations", m.processed);
    counter("jobq_completed_total", "Jobs completed", m.completed);
    counter("jobq_failed_total", "Jobs failed", m.failed);
    counter("jobq_retried_total", "Retries scheduled", m.retried);
    counter("jobq_timed_out_total", "Attempts timed out", m.timed_out);
    counter("jobq_rejected_total", "Submissions rejected (full)", m.rejected);
    gauge("jobq_pending", "Queued + running jobs", static_cast<double>(m.pending));
    gauge("jobq_in_flight", "Handlers executing now",
          static_cast<double>(m.in_flight));
    os << "# HELP jobq_queue_latency_ms Enqueue-eligible to claim latency\n"
       << "# TYPE jobq_queue_latency_ms summary\n"
       << "jobq_queue_latency_ms{quantile=\"0.5\"} " << m.latency_p50_ms << '\n'
       << "jobq_queue_latency_ms{quantile=\"0.95\"} " << m.latency_p95_ms << '\n'
       << "jobq_queue_latency_ms{quantile=\"0.99\"} " << m.latency_p99_ms << '\n'
       << "jobq_queue_latency_ms_count " << m.latency_samples << '\n';
    return os.str();
}

std::string JobQueue::metrics_json() {
    const Metrics m = metrics();
    std::ostringstream os;
    os << '{'
       << "\"submitted\":" << m.submitted << ','
       << "\"processed\":" << m.processed << ','
       << "\"completed\":" << m.completed << ','
       << "\"failed\":" << m.failed << ','
       << "\"retried\":" << m.retried << ','
       << "\"timed_out\":" << m.timed_out << ','
       << "\"rejected\":" << m.rejected << ','
       << "\"pending\":" << m.pending << ','
       << "\"in_flight\":" << m.in_flight << ','
       << "\"latency_p50_ms\":" << m.latency_p50_ms << ','
       << "\"latency_p95_ms\":" << m.latency_p95_ms << ','
       << "\"latency_p99_ms\":" << m.latency_p99_ms << ','
       << "\"latency_samples\":" << m.latency_samples
       << '}';
    return os.str();
}

}  // namespace jobq
