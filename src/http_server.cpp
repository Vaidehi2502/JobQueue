#include "http_server.hpp"

#include <atomic>
#include <chrono>
#include <thread>

#include "job_queue.hpp"

// Single-header third-party libraries (fetched by CMake). Kept out of the
// public header via the pimpl in Impl below.
#include "httplib.h"
#include "json.hpp"

namespace jobq {

using nlohmann::json;

namespace {

json job_to_json(const Job& j) {
    return json{
        {"id", j.id},
        {"type", j.type},
        {"payload", j.payload},
        {"state", to_string(j.state)},
        {"attempts", j.attempts},
        {"max_attempts", j.max_attempts},
        {"priority", j.priority},
        {"timeout_ms", j.timeout_ms},
        {"available_at", j.available_at},
        {"created_at", j.created_at},
        {"updated_at", j.updated_at},
        {"last_error", j.last_error},
    };
}

void send_json(httplib::Response& res, int status, const json& body) {
    res.status = status;
    res.set_content(body.dump(), "application/json");
}

void send_error(httplib::Response& res, int status, const std::string& msg) {
    send_json(res, status, json{{"error", msg}});
}

}  // namespace

struct HttpServer::Impl {
    explicit Impl(JobQueue& q) : queue(q) { setup_routes(); }

    JobQueue& queue;
    httplib::Server svr;
    std::thread thread;
    std::atomic<bool> running{false};

    void setup_routes();
};

void HttpServer::Impl::setup_routes() {
    svr.Get("/healthz", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("ok", "text/plain");
    });

    svr.Get("/stats", [this](const httplib::Request&, httplib::Response& res) {
        const auto c = queue.counts();
        send_json(res, 200,
                  json{{"queued", c.queued},
                       {"running", c.running},
                       {"completed", c.completed},
                       {"failed", c.failed},
                       {"pending", c.pending()}});
    });

    svr.Get("/metrics", [this](const httplib::Request&, httplib::Response& res) {
        res.set_content(queue.metrics_prometheus(), "text/plain; version=0.0.4");
    });

    svr.Get("/metrics.json",
            [this](const httplib::Request&, httplib::Response& res) {
                res.set_content(queue.metrics_json(), "application/json");
            });

    // POST /jobs — submit a job.
    svr.Post("/jobs", [this](const httplib::Request& req,
                             httplib::Response& res) {
        json body = json::parse(req.body, nullptr, /*allow_exceptions=*/false);
        if (body.is_discarded() || !body.is_object()) {
            return send_error(res, 400, "body must be a JSON object");
        }
        if (!body.contains("type") || !body["type"].is_string() ||
            body["type"].get<std::string>().empty()) {
            return send_error(res, 400, "'type' (non-empty string) is required");
        }

        const std::string type = body["type"].get<std::string>();

        // payload: accept a string verbatim, or serialize any other JSON value.
        std::string payload;
        if (body.contains("payload")) {
            payload = body["payload"].is_string()
                          ? body["payload"].get<std::string>()
                          : body["payload"].dump();
        }

        SubmitOptions opts;
        auto read_int = [&](const char* key, auto& dst) {
            if (body.contains(key) && body[key].is_number_integer()) {
                dst = body[key].get<std::remove_reference_t<decltype(dst)>>();
            }
        };
        read_int("max_attempts", opts.max_attempts);
        read_int("priority", opts.priority);
        if (body.contains("delay_ms") && body["delay_ms"].is_number_integer()) {
            opts.delay = std::chrono::milliseconds(body["delay_ms"].get<int64_t>());
        }
        if (body.contains("timeout_ms") &&
            body["timeout_ms"].is_number_integer()) {
            opts.timeout =
                std::chrono::milliseconds(body["timeout_ms"].get<int64_t>());
        }
        if (opts.max_attempts < 1) {
            return send_error(res, 400, "'max_attempts' must be >= 1");
        }

        // Non-blocking: refuse with 503 when the queue is at capacity rather
        // than holding the request thread.
        auto id = queue.try_submit(type, payload, opts);
        if (!id) return send_error(res, 503, "queue is at capacity");

        auto job = queue.status(*id);
        send_json(res, 201, job ? job_to_json(*job) : json{{"id", *id}});
    });

    // GET /jobs/{id}
    svr.Get(R"(/jobs/(\d+))", [this](const httplib::Request& req,
                                     httplib::Response& res) {
        const int64_t id = std::stoll(req.matches[1].str());
        auto job = queue.status(id);
        if (!job) return send_error(res, 404, "job not found");
        send_json(res, 200, job_to_json(*job));
    });

    // GET /jobs?state=&limit=
    svr.Get("/jobs", [this](const httplib::Request& req,
                            httplib::Response& res) {
        std::optional<JobState> filter;
        if (req.has_param("state")) {
            JobState st;
            if (!parse_state(req.get_param_value("state"), st)) {
                return send_error(res, 400, "unknown 'state'");
            }
            filter = st;
        }
        int limit = 1000;
        if (req.has_param("limit")) {
            try {
                limit = std::max(1, std::stoi(req.get_param_value("limit")));
            } catch (...) {
                return send_error(res, 400, "'limit' must be an integer");
            }
        }
        json arr = json::array();
        for (const auto& j : queue.list(filter, limit)) arr.push_back(job_to_json(j));
        send_json(res, 200, json{{"jobs", arr}, {"count", arr.size()}});
    });
}

HttpServer::HttpServer(JobQueue& queue)
    : impl_(std::make_unique<Impl>(queue)) {}

HttpServer::~HttpServer() { stop(); }

bool HttpServer::listen(const std::string& host, int port) {
    impl_->running = true;
    bool ok = impl_->svr.listen(host, port);
    impl_->running = false;
    return ok;
}

int HttpServer::listen_ephemeral(const std::string& host) {
    const int port = impl_->svr.bind_to_any_port(host);
    if (port <= 0) return -1;
    impl_->running = true;
    impl_->thread = std::thread([this] { impl_->svr.listen_after_bind(); });
    // Wait until the server is actually accepting, so callers can connect.
    for (int i = 0; i < 1000 && !impl_->svr.is_running(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return port;
}

void HttpServer::stop() {
    impl_->svr.stop();
    if (impl_->thread.joinable()) impl_->thread.join();
    impl_->running = false;
}

}  // namespace jobq
