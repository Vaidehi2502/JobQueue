#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "job.hpp"

struct sqlite3;

namespace jobq {

// Options for a single submission.
struct SubmitOptions {
    int max_attempts = 3;
    std::chrono::milliseconds delay{0};  // become eligible after this delay
};

// Thread-safe SQLite-backed persistence layer. A single connection is guarded
// by an internal mutex, so every public method is safe to call concurrently.
// Job *handlers* run outside this class, so holding the DB lock never blocks on
// application work.
//
// SQLite is the single source of truth for job state. Claims are atomic
// (one worker can never claim a job another worker already holds).
class JobStore {
public:
    // Opens (creating if needed) the database at `path`. Use ":memory:" for an
    // ephemeral store. Throws std::runtime_error on failure.
    explicit JobStore(const std::string& path);
    ~JobStore();

    JobStore(const JobStore&) = delete;
    JobStore& operator=(const JobStore&) = delete;

    // Inserts a new Queued job and returns its assigned id.
    int64_t insert(const std::string& type, const std::string& payload,
                   const SubmitOptions& opts);

    // Atomically claims the single eligible job with the earliest availability
    // (state == Queued and available_at <= now_ms), transitioning it to Running
    // and incrementing attempts. Returns the claimed job, or nullopt if none is
    // currently eligible.
    std::optional<Job> claim_next(int64_t now_ms);

    void mark_completed(int64_t id);

    // Marks a job permanently Failed with an error message.
    void mark_failed(int64_t id, const std::string& error);

    // Returns a Running job to Queued, eligible again at `available_at_ms`.
    void schedule_retry(int64_t id, int64_t available_at_ms,
                        const std::string& error);

    // Epoch ms of the soonest-eligible Queued job, or nullopt if none queued.
    // Used by workers to size their timed wait for delayed work.
    std::optional<int64_t> next_available_at();

    // Startup reconciliation: any job left Running by a previous process (it
    // crashed mid-execution) is returned to Queued so it can be retried.
    // Returns the number of jobs recovered.
    int recover_interrupted();

    std::optional<Job> get(int64_t id);
    std::vector<Job> list(std::optional<JobState> filter = std::nullopt,
                          int limit = 1000);

    // Count of jobs in each state, for status reporting and tests.
    struct Counts {
        int64_t queued = 0;
        int64_t running = 0;
        int64_t completed = 0;
        int64_t failed = 0;
    };
    Counts counts();

private:
    void exec(const char* sql);
    void init_schema();

    sqlite3* db_ = nullptr;
    std::mutex mutex_;
};

}  // namespace jobq
