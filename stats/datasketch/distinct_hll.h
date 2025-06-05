#pragma once

#include <cstdint>

#include "hll.hpp"

namespace stats {

class HLLDistinctCounter {
  static constexpr int32_t kDefaultLgK = 11;

 public:
  // sketch can hold 2^lg_config_k rows
  explicit HLLDistinctCounter(int32_t lg_config_k = kDefaultLgK) : sketch_(lg_config_k) {}

  void AppendValue(const int64_t value) { sketch_.update(value); }
  void AppendValue(const void* data, uint64_t size) { sketch_.update(data, size); }

  uint64_t GetDistinctValuesCount() const { return sketch_.get_estimate(); }

  datasketches::hll_sketch& GetSketch() { return sketch_; }
  const datasketches::hll_sketch& GetSketch() const { return sketch_; }

 private:
  datasketches::hll_sketch sketch_;
};

}  // namespace stats
