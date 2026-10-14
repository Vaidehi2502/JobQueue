#include "job_store.hpp"

#include <sqlite3.h>

#include <stdexcept>
#include <string>

namespace jobq {

namespace {

// Column list shared by every SELECT / RETURNING clause; read_job() depends on
// this exact order.
constexpr const char* kSelectColumns =
    "id, type, payload, state, attempts, max_attempts, priority, timeout_ms, "
    "available_at, created_at, updated_at, last_error";

void bind_int(sqlite3_stmt* s, int i, int64_t v) {
    sqlite3_bind_int64(s, i, v);
}
void bind_text(sqlite3_stmt* s, int i, const std::string& v) {
    // SQLITE_TRANSIENT: copy, since the std::string may be gone before step.
    sqlite3_bind_text(s, i, v.data(), static_cast<int>(v.size()),
                      SQLITE_TRANSIENT);
}

std::string col_text(sqlite3_stmt* s, int i) {
    const unsigned char* t = sqlite3_column_text(s, i);
    if (!t) return {};
    return std::string(reinterpret_cast<const char*>(t),
                       sqlite3_column_bytes(s, i));
}

Job read_job(sqlite3_stmt* s) {
    Job j;
    j.id = sqlite3_column_int64(s, 0);
    j.type = col_text(s, 1);
    j.payload = col_text(s, 2);
    JobState st = JobState::Queued;
    parse_state(col_text(s, 3), st);
    j.state = st;
    j.attempts = static_cast<int>(sqlite3_column_int64(s, 4));
    j.max_attempts = static_cast<int>(sqlite3_column_int64(s, 5));
    j.priority = static_cast<int>(sqlite3_column_int64(s, 6));
    j.timeout_ms = sqlite3_column_int64(s, 7);
    j.available_at = sqlite3_column_int64(s, 8);
    j.created_at = sqlite3_column_int64(s, 9);
    j.updated_at = sqlite3_column_int64(s, 10);
    j.last_error = col_text(s, 11);
    if (j.timeout_ms > 0) j.deadline_ms = j.updated_at + j.timeout_ms;
    return j;
}

// Resets a cached statement (and clears its bindings) when it leaves scope, so
// the next caller starts clean.
struct ScopedReset {
    sqlite3_stmt* s;
    ~ScopedReset() {
        sqlite3_reset(s);
        sqlite3_clear_bindings(s);
    }
};

// One-shot prepared statement for rarely-used dynamic SQL (list/counts/recover),
// finalized on scope exit.
class OneShot {
public:
    OneShot(sqlite3* db, const std::string& sql) : db_(db) {
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &s_, nullptr) != SQLITE_OK) {
            throw std::runtime_error(std::string("prepare failed: ") +
                                     sqlite3_errmsg(db));
        }
    }
    ~OneShot() { sqlite3_finalize(s_); }
    OneShot(const OneShot&) = delete;
    OneShot& operator=(const OneShot&) = delete;
    sqlite3_stmt* get() { return s_; }

private:
    sqlite3* db_;
    sqlite3_stmt* s_ = nullptr;
};

// Steps once; true if a row is available, false when done; throws on error.
bool step_row(sqlite3* db, sqlite3_stmt* s) {
    int rc = sqlite3_step(s);
    if (rc == SQLITE_ROW) return true;
    if (rc == SQLITE_DONE) return false;
    throw std::runtime_error(std::string("step failed: ") + sqlite3_errmsg(db));
}

}  // namespace

sqlite3_stmt* JobStore::prepare(const char* sql) {
    sqlite3_stmt* s = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &s, nullptr) != SQLITE_OK) {
        throw std::runtime_error(std::string("prepare failed: ") +
                                 sqlite3_errmsg(db_));
    }
    return s;
}

JobStore::JobStore(const std::string& path) {
    if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK) {
        std::string msg = db_ ? sqlite3_errmsg(db_) : "out of memory";
        sqlite3_close(db_);
        throw std::runtime_error("cannot open database: " + msg);
    }
    // WAL improves read/write concurrency; busy_timeout lets SQLite retry
    // instead of returning SQLITE_BUSY immediately under contention.
    exec("PRAGMA journal_mode=WAL;");
    exec("PRAGMA synchronous=NORMAL;");
    exec("PRAGMA foreign_keys=ON;");
    sqlite3_busy_timeout(db_, 5000);
    init_schema();

    // Prepare hot-path statements once; reused (reset + rebound) on every call.
    st_insert_ = prepare(
        "INSERT INTO jobs (type, payload, state, attempts, max_attempts, "
        "priority, timeout_ms, available_at, created_at, updated_at, last_error) "
        "VALUES (?, ?, 'queued', 0, ?, ?, ?, ?, ?, ?, '');");
    st_claim_ = prepare(
        "UPDATE jobs SET state='running', attempts=attempts+1, updated_at=? "
        "WHERE id = ("
        "  SELECT id FROM jobs WHERE state='queued' AND available_at<=? "
        "  ORDER BY priority DESC, available_at ASC, id ASC LIMIT 1"
        ") "
        "RETURNING id, type, payload, state, attempts, max_attempts, priority, "
        "timeout_ms, available_at, created_at, updated_at, last_error;");
    st_complete_ = prepare(
        "UPDATE jobs SET state='completed', updated_at=?, last_error='' "
        "WHERE id=? AND state='running' AND attempts=?;");
    st_fail_ = prepare(
        "UPDATE jobs SET state='failed', updated_at=?, last_error=? "
        "WHERE id=? AND state='running' AND attempts=?;");
    st_retry_ = prepare(
        "UPDATE jobs SET state='queued', available_at=?, updated_at=?, "
        "last_error=? WHERE id=? AND state='running' AND attempts=?;");
    st_next_ = prepare(
        "SELECT MIN(available_at) FROM jobs WHERE state='queued';");
    st_get_ = prepare((std::string("SELECT ") + kSelectColumns +
                       " FROM jobs WHERE id=?;")
                          .c_str());
    st_timed_out_ = prepare(
        (std::string("SELECT ") + kSelectColumns +
         " FROM jobs WHERE state='running' AND timeout_ms>0 AND "
         "(updated_at + timeout_ms) <= ?;")
            .c_str());
}

JobStore::~JobStore() {
    sqlite3_finalize(st_insert_);
    sqlite3_finalize(st_claim_);
    sqlite3_finalize(st_complete_);
    sqlite3_finalize(st_fail_);
    sqlite3_finalize(st_retry_);
    sqlite3_finalize(st_next_);
    sqlite3_finalize(st_get_);
    sqlite3_finalize(st_timed_out_);
    sqlite3_close(db_);
}

void JobStore::exec(const char* sql) {
    char* err = nullptr;
    if (sqlite3_exec(db_, sql, nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "unknown error";
        sqlite3_free(err);
        throw std::runtime_error("exec failed: " + msg);
    }
}

void JobStore::init_schema() {
    exec(
        "CREATE TABLE IF NOT EXISTS jobs ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  type TEXT NOT NULL,"
        "  payload TEXT NOT NULL DEFAULT '',"
        "  state TEXT NOT NULL,"
        "  attempts INTEGER NOT NULL DEFAULT 0,"
        "  max_attempts INTEGER NOT NULL DEFAULT 1,"
        "  priority INTEGER NOT NULL DEFAULT 0,"
        "  timeout_ms INTEGER NOT NULL DEFAULT 0,"
        "  available_at INTEGER NOT NULL,"
        "  created_at INTEGER NOT NULL,"
        "  updated_at INTEGER NOT NULL,"
        "  last_error TEXT NOT NULL DEFAULT ''"
        ");");
    // Index drives claim_next's "best eligible queued job" lookup.
    exec(
        "CREATE INDEX IF NOT EXISTS idx_jobs_ready "
        "ON jobs(state, priority DESC, available_at);");
}

int64_t JobStore::insert(const std::string& type, const std::string& payload,
                         const SubmitOptions& opts) {
    std::lock_guard<std::mutex> lock(mutex_);
    ScopedReset guard{st_insert_};
    const int64_t now = now_ms();
    const int64_t available = now + opts.delay.count();

    bind_text(st_insert_, 1, type);
    bind_text(st_insert_, 2, payload);
    bind_int(st_insert_, 3, opts.max_attempts);
    bind_int(st_insert_, 4, opts.priority);
    bind_int(st_insert_, 5, opts.timeout.count());
    bind_int(st_insert_, 6, available);
    bind_int(st_insert_, 7, now);
    bind_int(st_insert_, 8, now);
    step_row(db_, st_insert_);
    return sqlite3_last_insert_rowid(db_);
}

std::optional<Job> JobStore::claim_next(int64_t now) {
    std::lock_guard<std::mutex> lock(mutex_);
    ScopedReset guard{st_claim_};
    bind_int(st_claim_, 1, now);
    bind_int(st_claim_, 2, now);
    if (!step_row(db_, st_claim_)) return std::nullopt;
    return read_job(st_claim_);
}

bool JobStore::mark_completed(int64_t id, int expected_attempts) {
    std::lock_guard<std::mutex> lock(mutex_);
    ScopedReset guard{st_complete_};
    bind_int(st_complete_, 1, now_ms());
    bind_int(st_complete_, 2, id);
    bind_int(st_complete_, 3, expected_attempts);
    step_row(db_, st_complete_);
    return sqlite3_changes(db_) > 0;
}

bool JobStore::mark_failed(int64_t id, const std::string& error,
                           int expected_attempts) {
    std::lock_guard<std::mutex> lock(mutex_);
    ScopedReset guard{st_fail_};
    bind_int(st_fail_, 1, now_ms());
    bind_text(st_fail_, 2, error);
    bind_int(st_fail_, 3, id);
    bind_int(st_fail_, 4, expected_attempts);
    step_row(db_, st_fail_);
    return sqlite3_changes(db_) > 0;
}

bool JobStore::schedule_retry(int64_t id, int64_t available_at_ms,
                              const std::string& error, int expected_attempts) {
    std::lock_guard<std::mutex> lock(mutex_);
    ScopedReset guard{st_retry_};
    bind_int(st_retry_, 1, available_at_ms);
    bind_int(st_retry_, 2, now_ms());
    bind_text(st_retry_, 3, error);
    bind_int(st_retry_, 4, id);
    bind_int(st_retry_, 5, expected_attempts);
    step_row(db_, st_retry_);
    return sqlite3_changes(db_) > 0;
}

std::optional<int64_t> JobStore::next_available_at() {
    std::lock_guard<std::mutex> lock(mutex_);
    ScopedReset guard{st_next_};
    if (!step_row(db_, st_next_)) return std::nullopt;
    if (sqlite3_column_type(st_next_, 0) == SQLITE_NULL) return std::nullopt;
    return sqlite3_column_int64(st_next_, 0);
}

std::vector<Job> JobStore::find_timed_out(int64_t now) {
    std::lock_guard<std::mutex> lock(mutex_);
    ScopedReset guard{st_timed_out_};
    bind_int(st_timed_out_, 1, now);
    std::vector<Job> out;
    while (step_row(db_, st_timed_out_)) out.push_back(read_job(st_timed_out_));
    return out;
}

int JobStore::recover_interrupted() {
    std::lock_guard<std::mutex> lock(mutex_);
    OneShot s(db_,
              "UPDATE jobs SET state='queued', updated_at=" +
                  std::to_string(now_ms()) +
                  ", last_error='recovered after interruption' "
                  "WHERE state='running';");
    step_row(db_, s.get());
    return sqlite3_changes(db_);
}

std::optional<Job> JobStore::get(int64_t id) {
    std::lock_guard<std::mutex> lock(mutex_);
    ScopedReset guard{st_get_};
    bind_int(st_get_, 1, id);
    if (!step_row(db_, st_get_)) return std::nullopt;
    return read_job(st_get_);
}

std::vector<Job> JobStore::list(std::optional<JobState> filter, int limit) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string sql = std::string("SELECT ") + kSelectColumns + " FROM jobs";
    if (filter) sql += " WHERE state=?";
    sql += " ORDER BY id ASC LIMIT ?;";

    OneShot s(db_, sql);
    int idx = 1;
    if (filter) bind_text(s.get(), idx++, std::string(to_string(*filter)));
    bind_int(s.get(), idx, limit);

    std::vector<Job> out;
    while (step_row(db_, s.get())) out.push_back(read_job(s.get()));
    return out;
}

JobStore::Counts JobStore::counts() {
    std::lock_guard<std::mutex> lock(mutex_);
    Counts c;
    OneShot s(db_, "SELECT state, COUNT(*) FROM jobs GROUP BY state;");
    while (step_row(db_, s.get())) {
        const std::string state = col_text(s.get(), 0);
        const int64_t n = sqlite3_column_int64(s.get(), 1);
        if (state == "queued") c.queued = n;
        else if (state == "running") c.running = n;
        else if (state == "completed") c.completed = n;
        else if (state == "failed") c.failed = n;
    }
    return c;
}

}  // namespace jobq
