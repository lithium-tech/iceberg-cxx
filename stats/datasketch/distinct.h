#pragma once

#include <cstdint>
#include <utility>
#include <variant>

#include "stats/datasketch/distinct_hll.h"
#include "stats/datasketch/distinct_naive.h"
#include "stats/datasketch/distinct_theta.h"

namespace stats {

class GenericDistinctCounterSketch {
 public:
  using VariantType = std::variant<HLLDistinctCounter, ThetaDistinctCounter, NaiveDistinctCounter>;

  explicit GenericDistinctCounterSketch(const VariantType& other) : sketch_(other) {}
  explicit GenericDistinctCounterSketch(VariantType&& other) : sketch_(std::move(other)) {}

  void AppendValue(const int64_t value) {
    std::visit([value](auto&& sketch) { sketch.AppendValue(value); }, sketch_);
  }
  void AppendValue(const void* data, uint64_t size) {
    std::visit([data, size](auto&& sketch) { sketch.AppendValue(data, size); }, sketch_);
  }

  uint64_t GetDistinctValuesCount() const {
    return std::visit([](auto&& sketch) { return sketch.GetDistinctValuesCount(); }, sketch_);
  }

  const VariantType& GetSketch() const { return sketch_; }

 private:
  VariantType sketch_;
};

}  // namespace stats
