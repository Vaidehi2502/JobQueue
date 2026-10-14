#include "job_store.hpp"

#include <sqlite3.h>

#include <stdexcept>
#include <string>

namespace jobq {

namespace {

// RAII wrapper for a prepared statement so early returns / exceptions always
// finalize it.
class Stmt {
public:
    Stmt(sqlite3* db, const char* sql) : db_(db) {
        if (sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr) != SQLITE_OK) {
            throw std::runtime_error(std::string("prepare failed: ") +
                                     sqlite3_errmsg(db));
        }
    }
    ~Stmt() { sqlite3_finalize(stmt_); }

    Stmt(const Stmt&) = delete;
    Stmt& operator=(const Stmt&) = delete;

    void bind(int i, int64_t v) { sqlite3_bind_int64(stmt_, i, v); }
    void bind(int i, const std::string& v) {
        // SQLITE_TRANSIENT: copy the string, since the std::string may be gone
        // before the statement is executed.
        sqlite3_bind_text(stmt_, i, v.data(), static_cast<int>(v.size()),
                          SQLITE_TRANSIENT);
    }

    // Steps once; returns true if a row is available, false when done.
    bool step() {
        int rc = sqlite3_step(stmt_);
        if (rc == SQLITE_ROW) return true;
        if (rc == SQLITE_DONE) return false;
        throw std::runtime_error(std::string("step failed: ") +
                                 sqlite3_errmsg(db_));
    }

    int64_t col_int(int i) { return sqlite3_column_int64(stmt_, i); }
    std::string col_text(int i) {
        const unsigned char* t = sqlite3_column_text(stmt_, i);
        if (!t) return {};
        return std::string(reinterpret_cast<const char*>(t),
                           sqlite3_column_bytes(stmt_, i));
    }

    sqlite3_stmt* raw() { return stmt_; }

private:
    sqlite3* db_;
    sqlite3_stmt* stmt_ = nullptr;
};

Job read_job(Stmt& s) {
    Job j;
    j.id = s.col_int(0);
    j.type = s.col_text(1);
    j.payload = s.col_text(2);
    JobState st = JobState::Queued;
    parse_state(s.col_text(3), st);
    j.state = st;
    j.attempts = static_cast<int>(s.col_int(4));
    j.max_attempts = static_cast<int>(s.col_int(5));
    j.available_at = s.col_int(6);
    j.created_at = s.col_int(7);
    j.updated_at = s.col_int(8);
    j.last_error = s.col_text(9);
    return j;
}

constexpr const char* kSelectColumns =
    "id, type, payload, state, attempts, max_attempts, available_at, "
    "created_at, updated_at, last_error";

}  // namespace

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
}

JobStore::~JobStore() { sqlite3_close(db_); }

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
        "  available_at INTEGER NOT NULL,"
        "  created_at INTEGER NOT NULL,"
        "  updated_at INTEGER NOT NULL,"
        "  last_error TEXT NOT NULL DEFAULT ''"
        ");");
    // Index drives claim_next's "earliest eligible queued job" lookup.
    exec(
        "CREATE INDEX IF NOT EXISTS idx_jobs_ready "
        "ON jobs(state, available_at);");
}

int64_t JobStore::insert(const std::string& type, const std::string& payload,
                         const SubmitOptions& opts) {
    std::lock_guard<std::mutex> lock(mutex_);
    const int64_t now = now_ms();
    const int64_t available = now + opts.delay.count();

    Stmt s(db_,
           "INSERT INTO jobs (type, payload, state, attempts, max_attempts, "
           "available_at, created_at, updated_at, last_error) "
           "VALUES (?, ?, 'queued', 0, ?, ?, ?, ?, '');");
    s.bind(1, type);
    s.bind(2, payload);
    s.bind(3, static_cast<int64_t>(opts.max_attempts));
    s.bind(4, available);
    s.bind(5, now);
    s.bind(6, now);
    s.step();
    return sqlite3_last_insert_rowid(db_);
}

std::optional<Job> JobStore::claim_next(int64_t now) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Atomic claim: transition exactly one eligible job to running and bump its
    // attempt counter, returning the updated row. RETURNING makes this a single
    // statement so two workers can never select the same row.
    Stmt s(db_,
           "UPDATE jobs SET state='running', attempts=attempts+1, updated_at=? "
           "WHERE id = ("
           "  SELECT id FROM jobs WHERE state='queued' AND available_at<=? "
           "  ORDER BY available_at ASC, id ASC LIMIT 1"
           ") "
           "RETURNING id, type, payload, state, attempts, max_attempts, "
           "available_at, created_at, updated_at, last_error;");
    s.bind(1, now);
    s.bind(2, now);
    if (!s.step()) return std::nullopt;
    return read_job(s);
}

void JobStore::mark_completed(int64_t id) {
    std::lock_guard<std::mutex> lock(mutex_);
    Stmt s(db_,
           "UPDATE jobs SET state='completed', updated_at=?, last_error='' "
           "WHERE id=?;");
    s.bind(1, now_ms());
    s.bind(2, id);
    s.step();
}

void JobStore::mark_failed(int64_t id, const std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    Stmt s(db_,
           "UPDATE jobs SET state='failed', updated_at=?, last_error=? "
           "WHERE id=?;");
    s.bind(1, now_ms());
    s.bind(2, error);
    s.bind(3, id);
    s.step();
}

void JobStore::schedule_retry(int64_t id, int64_t available_at_ms,
                              const std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    Stmt s(db_,
           "UPDATE jobs SET state='queued', available_at=?, updated_at=?, "
           "last_error=? WHERE id=?;");
    s.bind(1, available_at_ms);
    s.bind(2, now_ms());
    s.bind(3, error);
    s.bind(4, id);
    s.step();
}

std::optional<int64_t> JobStore::next_available_at() {
    std::lock_guard<std::mutex> lock(mutex_);
    Stmt s(db_,
           "SELECT MIN(available_at) FROM jobs WHERE state='queued';");
    if (!s.step()) return std::nullopt;
    if (sqlite3_column_type(s.raw(), 0) == SQLITE_NULL) return std::nullopt;
    return s.col_int(0);
}

int JobStore::recover_interrupted() {
    std::lock_guard<std::mutex> lock(mutex_);
    Stmt s(db_,
           "UPDATE jobs SET state='queued', updated_at=?, "
           "last_error='recovered after interruption' "
           "WHERE state='running';");
    s.bind(1, now_ms());
    s.step();
    return sqlite3_changes(db_);
}

std::optional<Job> JobStore::get(int64_t id) {
    std::lock_guard<std::mutex> lock(mutex_);
    Stmt s(db_, (std::string("SELECT ") + kSelectColumns +
                 " FROM jobs WHERE id=?;")
                    .c_str());
    s.bind(1, id);
    if (!s.step()) return std::nullopt;
    return read_job(s);
}

std::vector<Job> JobStore::list(std::optional<JobState> filter, int limit) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string sql = std::string("SELECT ") + kSelectColumns + " FROM jobs";
    if (filter) sql += " WHERE state=?";
    sql += " ORDER BY id ASC LIMIT ?;";

    Stmt s(db_, sql.c_str());
    int idx = 1;
    if (filter) s.bind(idx++, std::string(to_string(*filter)));
    s.bind(idx, static_cast<int64_t>(limit));

    std::vector<Job> out;
    while (s.step()) out.push_back(read_job(s));
    return out;
}

JobStore::Counts JobStore::counts() {
    std::lock_guard<std::mutex> lock(mutex_);
    Counts c;
    Stmt s(db_, "SELECT state, COUNT(*) FROM jobs GROUP BY state;");
    while (s.step()) {
        const std::string state = s.col_text(0);
        const int64_t n = s.col_int(1);
        if (state == "queued") c.queued = n;
        else if (state == "running") c.running = n;
        else if (state == "completed") c.completed = n;
        else if (state == "failed") c.failed = n;
    }
    return c;
}

}  // namespace jobq
