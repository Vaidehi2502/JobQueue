#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>

namespace jobq {

using Clock = std::chrono::system_clock;
using TimePoint = Clock::time_point;

// Epoch milliseconds are used for all persisted timestamps so that times
// survive a process restart and compare correctly across runs.
inline int64_t to_epoch_ms(TimePoint tp) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               tp.time_since_epoch())
        .count();
}

inline TimePoint from_epoch_ms(int64_t ms) {
    return TimePoint{std::chrono::milliseconds{ms}};
}

inline int64_t now_ms() { return to_epoch_ms(Clock::now()); }

// Lifecycle states. A job is always in exactly one of these, and the
// persistence layer enforces the legal transitions documented in the README.
enum class JobState {
    Queued,     // accepted, waiting to become eligible / be claimed
    Running,    // claimed by a worker
    Completed,  // handler reported success
    Failed      // permanent failure or retries exhausted
};

const char* to_string(JobState state);
bool parse_state(const std::string& text, JobState& out);

// What a handler reports back. The queue decides retry vs. fail from this.
enum class JobOutcome {
    Success,            // done, mark Completed
    RetryableFailure,   // transient; retry with backoff if attempts remain
    PermanentFailure    // do not retry; mark Failed immediately
};

struct JobResult {
    JobOutcome outcome = JobOutcome::Success;
    std::string message;  // optional diagnostic, stored as last_error on failure

    static JobResult success() { return {JobOutcome::Success, {}}; }
    static JobResult retry(std::string msg = {}) {
        return {JobOutcome::RetryableFailure, std::move(msg)};
    }
    static JobResult fail(std::string msg = {}) {
        return {JobOutcome::PermanentFailure, std::move(msg)};
    }
};

// A unit of work. `id` is assigned by the store on insert. `payload` is an
// opaque application string (often JSON); the queue never interprets it.
struct Job {
    int64_t id = 0;
    std::string type;
    std::string payload;
    JobState state = JobState::Queued;
    int attempts = 0;        // completed execution attempts so far
    int max_attempts = 1;    // total attempts allowed before permanent failure
    int64_t available_at = 0;  // epoch ms; job is eligible once now >= this
    int64_t created_at = 0;    // epoch ms at submission
    int64_t updated_at = 0;    // epoch ms of last state change
    std::string last_error;
};

// Registered per job type. Runs outside any queue lock. Should be idempotent
// where external side effects are involved (see README on execution guarantees).
using JobHandler = std::function<JobResult(const Job&)>;

// Bounded retry with exponential backoff:
//   delay_n = min(max_delay, initial_delay * 2^(n-1))
// where n is the 1-based retry number (first retry is n = 1).
struct RetryPolicy {
    int max_attempts = 3;
    std::chrono::milliseconds initial_delay{200};
    std::chrono::milliseconds max_delay{30'000};

    std::chrono::milliseconds backoff_for_retry(int retry_number) const;
};

}  // namespace jobq
