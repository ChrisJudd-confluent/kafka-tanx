#include "logger.h"
#include <fstream>
#include <chrono>
#include <ctime>
#include <cstdio>

namespace {
    std::ofstream g_file;
}

namespace NetLog {

void Init(const std::string& path) {
    g_file.open(path, std::ios::app);
    Write("=== KafkaTanx started ===");
}

void Write(const std::string& line) {
    if (!g_file.is_open()) return;

    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()).count() % 1000;

    char stamp[32];
    std::strftime(stamp, sizeof(stamp), "%H:%M:%S", std::localtime(&t));
    char buf[16];
    std::snprintf(buf, sizeof(buf), ".%03d", (int)ms);

    g_file << stamp << buf << "  " << line << "\n";
    g_file.flush();
}

}  // namespace NetLog
