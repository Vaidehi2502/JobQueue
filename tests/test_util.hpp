#pragma once

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>

// A unique temporary database path that deletes itself (and SQLite's -wal/-shm
// sidecar files) on destruction. Used by tests that need persistence to
// survive across JobStore/JobQueue instances (":memory:" cannot).
class TempDb {
public:
    TempDb() {
        static std::atomic<int> counter{0};
        const auto pid = static_cast<long>(::getpid());
        const auto n = counter.fetch_add(1);
        path_ = (std::filesystem::temp_directory_path() /
                 ("jobq_test_" + std::to_string(pid) + "_" +
                  std::to_string(n) + ".db"))
                    .string();
        remove();
    }
    ~TempDb() { remove(); }

    TempDb(const TempDb&) = delete;
    TempDb& operator=(const TempDb&) = delete;

    const std::string& path() const { return path_; }

private:
    void remove() {
        std::error_code ec;
        std::filesystem::remove(path_, ec);
        std::filesystem::remove(path_ + "-wal", ec);
        std::filesystem::remove(path_ + "-shm", ec);
    }
    std::string path_;
};
