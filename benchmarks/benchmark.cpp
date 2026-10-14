// Benchmark harness for the job queue.
//
// Measures end-to-end throughput (completed jobs / wall-clock second) and
// queue latency percentiles for a no-op handler, across a configurable number
// of jobs and workers, for both an in-memory store and an on-disk SQLite store
// (to show persistence overhead).
//
// Results depend on hardware, compiler, build type, and SQLite settings; this
// harness measures, it does not assert any particular number.
//
// Usage: benchmark [num_jobs] [num_workers]

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <string>

#include "job_queue.hpp"

using namespace jobq;

namespace {

struct Result {
    double elapsed_s = 0;
    double throughput = 0;
    Metrics metrics;
    int64_t completed = 0;
};

Result run_case(const std::string& db_path, int num_jobs, unsigned workers) {
    JobQueue q(db_path);
    q.register_handler("noop", [](const Job&) { return JobResult::success(); });
    q.start(workers);

    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < num_jobs; ++i) {
        q.submit("noop", "");
    }
    q.drain(std::chrono::minutes(5));
    const auto end = std::chrono::steady_clock::now();
    q.shutdown();

    Result r;
    r.elapsed_s = std::chrono::duration<double>(end - start).count();
    r.completed = q.counts().completed;
    r.throughput = r.elapsed_s > 0 ? r.completed / r.elapsed_s : 0;
    r.metrics = q.metrics();
    return r;
}

void print_result(const char* label, const Result& r) {
    std::printf("%-22s completed=%-7lld  elapsed=%6.3fs  throughput=%10.1f jobs/s\n",
                label, static_cast<long long>(r.completed), r.elapsed_s,
                r.throughput);
    std::printf("%-22s latency p50/p95/p99 (ms) = %.3f / %.3f / %.3f  (n=%lld)\n",
                "", r.metrics.latency_p50_ms, r.metrics.latency_p95_ms,
                r.metrics.latency_p99_ms,
                static_cast<long long>(r.metrics.latency_samples));
}

}  // namespace

int main(int argc, char** argv) {
    const int num_jobs = argc > 1 ? std::stoi(argv[1]) : 50000;
    const unsigned workers =
        argc > 2 ? static_cast<unsigned>(std::stoul(argv[2])) : 4;

    std::cout << "cpp-job-queue benchmark\n"
              << "jobs=" << num_jobs << "  workers=" << workers << "\n\n";

    std::cout << "== In-memory store (no persistence I/O) ==\n";
    Result mem = run_case(":memory:", num_jobs, workers);
    print_result("in-memory", mem);

    const std::string db =
        (std::filesystem::temp_directory_path() / "jobq_benchmark.db").string();
    std::error_code ec;
    std::filesystem::remove(db, ec);
    std::filesystem::remove(db + "-wal", ec);
    std::filesystem::remove(db + "-shm", ec);

    std::cout << "\n== On-disk store (SQLite WAL) ==\n";
    Result disk = run_case(db, num_jobs, workers);
    print_result("on-disk", disk);

    std::filesystem::remove(db, ec);
    std::filesystem::remove(db + "-wal", ec);
    std::filesystem::remove(db + "-shm", ec);

    if (disk.throughput > 0) {
        std::cout << "\nPersistence overhead: "
                  << (mem.throughput / disk.throughput)
                  << "x slower on disk vs in-memory\n";
    }
    return 0;
}
