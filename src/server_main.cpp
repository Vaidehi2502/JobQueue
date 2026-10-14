// HTTP server entry point for the job queue.
//
// Starts a persistent JobQueue with a worker pool and serves the HTTP API
// (see http_server.hpp). Registered example handlers: "echo" (logs its payload
// and succeeds) and "sleep" (sleeps for the integer ms in its payload).
//
// Usage: job_queue_server [db_path] [port] [num_workers]
//
// Try it:
//   curl -XPOST localhost:8080/jobs -d '{"type":"echo","payload":"hi"}'
//   curl localhost:8080/jobs/1
//   curl localhost:8080/stats
//   curl localhost:8080/metrics

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <thread>

#include "http_server.hpp"
#include "job_queue.hpp"

using namespace jobq;

namespace {
std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop.store(true); }
}  // namespace

int main(int argc, char** argv) {
    const std::string db_path = argc > 1 ? argv[1] : "jobs.db";
    const int port = argc > 2 ? std::stoi(argv[2]) : 8080;
    const unsigned workers =
        argc > 3 ? static_cast<unsigned>(std::stoul(argv[3])) : 4;

    JobQueue queue(db_path);
    queue.set_logger(make_stderr_logger(LogLevel::Info));

    queue.register_handler("echo", [](const Job& j) {
        std::cout << "[echo] job " << j.id << ": " << j.payload << '\n';
        return JobResult::success();
    });
    queue.register_handler("sleep", [](const Job& j) {
        int ms = 0;
        try {
            ms = std::stoi(j.payload);
        } catch (...) {
            return JobResult::fail("payload must be an integer number of ms");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        return JobResult::success();
    });

    queue.start(workers);

    HttpServer server(queue);
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    std::cout << "job_queue serving on http://0.0.0.0:" << port
              << "  (db=" << db_path << ", workers=" << workers << ")\n"
              << "Press Ctrl-C to stop.\n";

    // Serve on a background thread so the main thread can watch for signals.
    std::thread serve([&] { server.listen("0.0.0.0", port); });
    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "\nShutting down...\n";
    server.stop();
    if (serve.joinable()) serve.join();
    queue.shutdown();
    return 0;
}
