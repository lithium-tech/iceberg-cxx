#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>

namespace stats {

class NaiveDistinctCounter {
 public:
  void AppendValue(const void* data, int64_t size) {
    string_values_.insert(std::string(reinterpret_cast<const char*>(data), size));
  }
  void AppendValue(const int64_t value) { int_values_.insert(value); }

  uint64_t GetDistinctValuesCount() const { return string_values_.size() + int_values_.size(); }

 private:
  std::unordered_set<std::string> string_values_;
  std::unordered_set<int64_t> int_values_;
};

}  // namespace stats
