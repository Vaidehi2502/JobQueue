// Integration tests for the HTTP API. A server is bound to an ephemeral port
// on a background thread; requests go through a real TCP client.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include "http_server.hpp"
#include "job_queue.hpp"

// Client side of cpp-httplib (same header, fetched by CMake).
#include "httplib.h"
#include "json.hpp"

using namespace jobq;
using namespace std::chrono_literals;
using nlohmann::json;

namespace {
// Spins up a queue + HTTP server for the duration of a test.
struct ServerFixture {
    JobQueue queue{":memory:"};
    HttpServer server{queue};
    int port = -1;

    ServerFixture() {
        queue.register_handler("ok", [](const Job&) { return JobResult::success(); });
        queue.start(2);
        port = server.listen_ephemeral("127.0.0.1");
    }
    ~ServerFixture() {
        server.stop();
        queue.shutdown();
    }
    httplib::Client client() { return httplib::Client("127.0.0.1", port); }
};
}  // namespace

TEST(Http, HealthAndStats) {
    ServerFixture f;
    ASSERT_GT(f.port, 0);
    auto c = f.client();

    auto health = c.Get("/healthz");
    ASSERT_TRUE(health);
    EXPECT_EQ(health->status, 200);
    EXPECT_EQ(health->body, "ok");

    auto stats = c.Get("/stats");
    ASSERT_TRUE(stats);
    EXPECT_EQ(stats->status, 200);
    auto body = json::parse(stats->body);
    EXPECT_TRUE(body.contains("completed"));
    EXPECT_TRUE(body.contains("pending"));
}

TEST(Http, SubmitAndFetchJob) {
    ServerFixture f;
    auto c = f.client();

    auto post = c.Post("/jobs", R"({"type":"ok","payload":"hello"})",
                       "application/json");
    ASSERT_TRUE(post);
    ASSERT_EQ(post->status, 201);
    auto created = json::parse(post->body);
    const int64_t id = created["id"].get<int64_t>();
    EXPECT_GT(id, 0);
    EXPECT_EQ(created["type"], "ok");
    EXPECT_EQ(created["payload"], "hello");

    // The job should run and complete shortly.
    ASSERT_TRUE(f.queue.drain(5s));

    auto get = c.Get(("/jobs/" + std::to_string(id)).c_str());
    ASSERT_TRUE(get);
    ASSERT_EQ(get->status, 200);
    auto fetched = json::parse(get->body);
    EXPECT_EQ(fetched["state"], "completed");
}

TEST(Http, MissingTypeIsBadRequest) {
    ServerFixture f;
    auto c = f.client();
    auto post = c.Post("/jobs", R"({"payload":"x"})", "application/json");
    ASSERT_TRUE(post);
    EXPECT_EQ(post->status, 400);

    auto bad = c.Post("/jobs", "not json", "application/json");
    ASSERT_TRUE(bad);
    EXPECT_EQ(bad->status, 400);
}

TEST(Http, UnknownJobIs404) {
    ServerFixture f;
    auto c = f.client();
    auto get = c.Get("/jobs/999999");
    ASSERT_TRUE(get);
    EXPECT_EQ(get->status, 404);
}

TEST(Http, ListAndFilterByState) {
    ServerFixture f;
    auto c = f.client();
    for (int i = 0; i < 3; ++i) {
        c.Post("/jobs", R"({"type":"ok","payload":"x"})", "application/json");
    }
    ASSERT_TRUE(f.queue.drain(5s));

    auto all = c.Get("/jobs");
    ASSERT_TRUE(all);
    ASSERT_EQ(all->status, 200);
    EXPECT_EQ(json::parse(all->body)["count"].get<int>(), 3);

    auto completed = c.Get("/jobs?state=completed");
    ASSERT_TRUE(completed);
    EXPECT_EQ(json::parse(completed->body)["count"].get<int>(), 3);

    auto none = c.Get("/jobs?state=failed");
    ASSERT_TRUE(none);
    EXPECT_EQ(json::parse(none->body)["count"].get<int>(), 0);
}

TEST(Http, CapacityReturns503) {
    JobQueue queue(":memory:");
    queue.set_capacity(2);
    // Handler blocks so pending stays at capacity.
    std::atomic<bool> release{false};
    queue.register_handler("block", [&](const Job&) {
        while (!release.load()) std::this_thread::sleep_for(2ms);
        return JobResult::success();
    });
    queue.start(1);
    HttpServer server(queue);
    const int port = server.listen_ephemeral("127.0.0.1");
    ASSERT_GT(port, 0);
    httplib::Client c("127.0.0.1", port);

    EXPECT_EQ(c.Post("/jobs", R"({"type":"block"})", "application/json")->status, 201);
    EXPECT_EQ(c.Post("/jobs", R"({"type":"block"})", "application/json")->status, 201);
    auto full = c.Post("/jobs", R"({"type":"block"})", "application/json");
    ASSERT_TRUE(full);
    EXPECT_EQ(full->status, 503);

    release.store(true);
    server.stop();
    queue.shutdown();
}

TEST(Http, MetricsEndpoints) {
    ServerFixture f;
    auto c = f.client();
    c.Post("/jobs", R"({"type":"ok"})", "application/json");
    ASSERT_TRUE(f.queue.drain(5s));

    auto prom = c.Get("/metrics");
    ASSERT_TRUE(prom);
    EXPECT_EQ(prom->status, 200);
    EXPECT_NE(prom->body.find("jobq_completed_total"), std::string::npos);

    auto mj = c.Get("/metrics.json");
    ASSERT_TRUE(mj);
    EXPECT_EQ(mj->status, 200);
    EXPECT_TRUE(json::parse(mj->body).contains("completed"));
}
