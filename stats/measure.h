#pragma once

#include <chrono>

namespace iceberg {

using TimePointClock = std::chrono::time_point<std::chrono::steady_clock>;
using DurationClock = std::chrono::steady_clock::duration;

class ScopedTimerClock {
 public:
  explicit ScopedTimerClock(DurationClock& result) : start_(std::chrono::steady_clock::now()), result_(result) {}
  ~ScopedTimerClock() { result_ += std::chrono::steady_clock::now() - start_; }

 private:
  TimePointClock start_;
  DurationClock& result_;
};

}  // namespace iceberg
