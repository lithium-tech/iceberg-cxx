#pragma once

#include <utility>

#include "stats/datasketch/distinct.h"
#include "stats/datasketch/distinct_theta.h"
#include "stats/naive/distinct.h"
#include "stats/parquet/sketch.h"

namespace stats {

using NaiveDistinctCounterWrapper = SketchWrapper<NaiveDistinctCounter>;

using HLLDistinctCounterWrapper = SketchWrapper<HLLDistinctCounter>;

using ThetaDistinctCounterWrapper = SketchWrapper<ThetaDistinctCounter>;

class CommonDistinctWrapper {
 public:
  explicit CommonDistinctWrapper(NaiveDistinctCounterWrapper&& naive) : impl_(std::move(naive)) {}
  explicit CommonDistinctWrapper(HLLDistinctCounterWrapper&& hll) : impl_(std::move(hll)) {}
  explicit CommonDistinctWrapper(ThetaDistinctCounterWrapper&& theta) : impl_(std::move(theta)) {}

  void SetFLBALength(int64_t length) {
    return std::visit([&](auto&& arg) { return arg.SetFLBALength(length); }, impl_);
  }

  void AppendValues(const void* data, uint64_t num_values, parquet::Type::type type) {
    return std::visit([&](auto&& arg) { return arg.AppendValues(data, num_values, type); }, impl_);
  }

  uint64_t GetNumberOfDistinctValues() const {
    return std::visit(
        [](auto&& arg) {
          using ArgType = std::decay_t<decltype(arg)>;
          using SketchType = ArgType::SketchType;
          return arg.Evaluate([](const SketchType& sketch) { return sketch.GetDistinctValuesCount(); });
        },
        impl_);
  }

  using SketchVariant =
      std::variant<HLLDistinctCounterWrapper, NaiveDistinctCounterWrapper, ThetaDistinctCounterWrapper>;

  const SketchVariant& GetState() const { return impl_; }

 private:
  SketchVariant impl_;
};

}  // namespace stats
