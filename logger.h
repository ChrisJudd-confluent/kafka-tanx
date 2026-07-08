#pragma once
#include <string>

// Minimal persistent logger for diagnosing connection drops after the fact.
// Every line is timestamped and flushed immediately so a crash or force-quit
// doesn't lose the tail of the log. Only called from the main thread (rdkafka
// callbacks fire synchronously inside poll()/consume(), never on a background
// thread here), so no locking.
namespace NetLog {
    void Init(const std::string& path);
    void Write(const std::string& line);
}
