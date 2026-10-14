#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "job.hpp"

struct sqlite3;
struct sqlite3_stmt;

namespace jobq {

// Options for a single submission.
struct SubmitOptions {
    int max_attempts = 3;
    int priority = 0;                    // higher runs first
    std::chrono::milliseconds delay{0};  // become eligible after this delay
    std::chrono::milliseconds timeout{0};  // per-attempt budget; 0 = none
};

// Thread-safe SQLite-backed persistence layer. A single connection is guarded
// by an internal mutex, so every public method is safe to call concurrently.
// Job *handlers* run outside this class, so holding the DB lock never blocks on
// application work.
//
// SQLite is the single source of truth for job state. Claims are atomic
// (one worker can never claim a job another worker already holds). The hot-path
// statements are prepared once and reused on every call.
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

    // Atomically claims the single best eligible job (state == Queued and
    // available_at <= now_ms), ordered by priority (desc) then availability,
    // transitioning it to Running and incrementing attempts. Returns the
    // claimed job, or nullopt if none is currently eligible.
    std::optional<Job> claim_next(int64_t now_ms);

    // Resolution methods are guarded against a superseded view: the update only
    // applies while the row is still Running with the attempt count the caller
    // observed at claim time. They return true if the row was updated, false if
    // the claim was already superseded (e.g. a watchdog timed the job out and
    // another worker re-claimed it). Pass expected_attempts from the claimed Job.
    bool mark_completed(int64_t id, int expected_attempts);
    bool mark_failed(int64_t id, const std::string& error, int expected_attempts);
    bool schedule_retry(int64_t id, int64_t available_at_ms,
                        const std::string& error, int expected_attempts);

    // Epoch ms of the soonest-eligible Queued job, or nullopt if none queued.
    // Used by workers to size their timed wait for delayed work.
    std::optional<int64_t> next_available_at();

    // Returns Running jobs whose per-attempt deadline (updated_at + timeout_ms)
    // has passed as of `now_ms`. The caller decides retry vs. fail and applies
    // it with the conditional resolution methods above.
    std::vector<Job> find_timed_out(int64_t now_ms);

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

        int64_t pending() const { return queued + running; }
    };
    Counts counts();

private:
    void exec(const char* sql);
    void init_schema();
    sqlite3_stmt* prepare(const char* sql);

    sqlite3* db_ = nullptr;
    std::mutex mutex_;

    // Cached prepared statements for hot-path operations (owned; finalized in
    // the destructor). Reset + rebound on each use under mutex_.
    sqlite3_stmt* st_insert_ = nullptr;
    sqlite3_stmt* st_claim_ = nullptr;
    sqlite3_stmt* st_complete_ = nullptr;
    sqlite3_stmt* st_fail_ = nullptr;
    sqlite3_stmt* st_retry_ = nullptr;
    sqlite3_stmt* st_next_ = nullptr;
    sqlite3_stmt* st_get_ = nullptr;
    sqlite3_stmt* st_timed_out_ = nullptr;
};

}  // namespace jobq
