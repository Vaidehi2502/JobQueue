#pragma once

#include <memory>
#include <string>

namespace jobq {

class JobQueue;

// A small HTTP front end over a JobQueue. The queue must outlive the server.
// Routes (all JSON unless noted):
//
//   POST /jobs            submit a job
//       body: {"type":"email","payload":"...","max_attempts":3,
//              "delay_ms":0,"priority":0,"timeout_ms":0}
//       201 -> the created job; 400 bad request; 503 queue full
//   GET  /jobs/{id}       fetch one job            200 / 404
//   GET  /jobs?state=&limit=   list jobs           200
//   GET  /stats           per-state counts         200
//   GET  /metrics         Prometheus text          200 (text/plain)
//   GET  /metrics.json    metrics as JSON          200
//   GET  /healthz         liveness                 200 (text/plain)
//
// The implementation is header-isolated (pimpl) so callers do not need the
// underlying HTTP/JSON libraries on their include path.
class HttpServer {
public:
    explicit HttpServer(JobQueue& queue);
    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    // Blocks serving host:port until stop() is called from another thread.
    // Returns false if the socket could not be bound.
    bool listen(const std::string& host, int port);

    // Binds an OS-chosen free port and serves on a background thread; returns
    // the chosen port, or -1 on failure. Intended for tests. Call stop() to
    // end. Only one listen()/listen_ephemeral() may be active at a time.
    int listen_ephemeral(const std::string& host = "127.0.0.1");

    // Stops serving and joins the background thread if one is running. Safe to
    // call more than once.
    void stop();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace jobq
