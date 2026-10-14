#include "job.hpp"

#include <algorithm>

namespace jobq {

const char* to_string(JobState state) {
    switch (state) {
        case JobState::Queued: return "queued";
        case JobState::Running: return "running";
        case JobState::Completed: return "completed";
        case JobState::Failed: return "failed";
    }
    return "unknown";
}

bool parse_state(const std::string& text, JobState& out) {
    if (text == "queued") { out = JobState::Queued; return true; }
    if (text == "running") { out = JobState::Running; return true; }
    if (text == "completed") { out = JobState::Completed; return true; }
    if (text == "failed") { out = JobState::Failed; return true; }
    return false;
}

std::chrono::milliseconds RetryPolicy::backoff_for_retry(int retry_number) const {
    if (retry_number < 1) retry_number = 1;

    // delay = initial_delay * 2^(retry_number - 1), clamped to max_delay.
    // Compute the multiplier with saturation so large retry numbers cannot
    // overflow into a small or negative delay.
    const int64_t initial = initial_delay.count();
    const int64_t cap = max_delay.count();

    int64_t delay = initial;
    for (int i = 1; i < retry_number; ++i) {
        if (delay >= cap || delay > cap / 2) {  // next doubling would reach/exceed cap
            delay = cap;
            break;
        }
        delay *= 2;
    }
    delay = std::min(delay, cap);
    return std::chrono::milliseconds{delay};
}

}  // namespace jobq
