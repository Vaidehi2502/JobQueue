#include "job_queue.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace jobq {

JobQueue::JobQueue(const std::string& db_path, RetryPolicy retry)
    : store_(std::make_unique<JobStore>(db_path)), retry_(retry) {}

JobQueue::~JobQueue() { shutdown(); }

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

    // Reconcile any work left Running by a previous process before workers
    // begin claiming, so interrupted jobs are not stranded.
    store_->recover_interrupted();

    workers_.reserve(num_workers);
    for (unsigned i = 0; i < num_workers; ++i) {
        workers_.emplace_back([this, i] { worker_loop(i); });
    }
}

int64_t JobQueue::submit(const std::string& type, const std::string& payload) {
    SubmitOptions opts;
    opts.max_attempts = retry_.max_attempts;
    return submit(type, payload, opts);
}

int64_t JobQueue::submit(const std::string& type, const std::string& payload,
                         const SubmitOptions& opts) {
    // Durable acceptance: the row is committed before we return an id. Only
    // after the store op releases its lock do we take wait_mutex_ to wake a
    // worker — this ordering is what keeps submit() deadlock-free against the
    // worker loop (which takes wait_mutex_ then the store lock).
    const int64_t id = store_->insert(type, payload, opts);
    submitted_.fetch_add(1, std::memory_order_relaxed);
    notify_work_available();
    return id;
}

void JobQueue::notify_work_available() {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    work_cv_.notify_one();
}

void JobQueue::worker_loop([[maybe_unused]] unsigned worker_id) {
    while (true) {
        std::optional<Job> job;
        {
            std::unique_lock<std::mutex> lock(wait_mutex_);
            if (stopping_.load(std::memory_order_acquire)) break;

            job = store_->claim_next(now_ms());
            if (!job) {
                // No eligible work. Sleep until a worker is notified, or until
                // the soonest delayed job becomes eligible. We hold wait_mutex_
                // across claim + wait, so a concurrent submit()/schedule_retry()
                // cannot notify between the empty claim and the wait (it blocks
                // on wait_mutex_ until we release it by waiting) — no missed
                // wakeup, and plain waits tolerate spurious ones via the loop.
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
            // Unknown type can never succeed; fail permanently rather than
            // spin-claiming the same job forever.
            result = JobResult::fail("no handler registered for type '" +
                                     job->type + "'");
        }

        processed_.fetch_add(1, std::memory_order_relaxed);
        handle_result(*job, result);
        in_flight_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void JobQueue::handle_result(const Job& job, const JobResult& result) {
    switch (result.outcome) {
        case JobOutcome::Success:
            store_->mark_completed(job.id);
            completed_.fetch_add(1, std::memory_order_relaxed);
            return;

        case JobOutcome::PermanentFailure:
            store_->mark_failed(job.id, result.message);
            failed_.fetch_add(1, std::memory_order_relaxed);
            return;

        case JobOutcome::RetryableFailure:
            // job.attempts already counts this attempt (claim incremented it).
            if (job.attempts >= job.max_attempts) {
                store_->mark_failed(
                    job.id, result.message.empty()
                                ? "retries exhausted"
                                : result.message + " (retries exhausted)");
                failed_.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            // Retry number n == attempts so far: first retry uses initial_delay.
            {
                const auto delay = retry_.backoff_for_retry(job.attempts);
                store_->schedule_retry(job.id, now_ms() + delay.count(),
                                       result.message);
                retried_.fetch_add(1, std::memory_order_relaxed);
                notify_work_available();
            }
            return;
    }
}

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
    // Idempotent: only the first call sets stopping_ and joins.
    bool expected = false;
    if (!stopping_.compare_exchange_strong(expected, true)) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(wait_mutex_);
        work_cv_.notify_all();
    }
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
}

std::optional<Job> JobQueue::status(int64_t id) { return store_->get(id); }

JobStore::Counts JobQueue::counts() { return store_->counts(); }

Metrics JobQueue::metrics() {
    Metrics m;
    m.submitted = submitted_.load();
    m.processed = processed_.load();
    m.completed = completed_.load();
    m.failed = failed_.load();
    m.retried = retried_.load();

    std::vector<double> samples;
    {
        std::lock_guard<std::mutex> lock(latency_mutex_);
        samples = latency_samples_;
    }
    m.latency_samples = static_cast<int64_t>(samples.size());
    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        auto pct = [&](double p) {
            // Nearest-rank percentile.
            size_t idx = static_cast<size_t>(
                std::ceil(p / 100.0 * samples.size()));
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

}  // namespace jobq
