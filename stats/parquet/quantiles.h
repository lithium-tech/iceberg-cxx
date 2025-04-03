#pragma once

#include <cstdint>

#include "parquet/types.h"
#include "stats/datasketch/quantiles.h"
#include "stats/parquet/sketch.h"

namespace stats {

class CommonQuantileWrapper {
 public:
  explicit CommonQuantileWrapper(stats::Type type) : sketch_(type) {}

  void SetFLBALength(int64_t length) { return sketch_.SetFLBALength(length); }

  void AppendValues(const void* data, uint64_t num_values, parquet::Type::type type) {
    return sketch_.AppendValues(data, num_values, type);
  }

  template <typename T>
  auto GetHistogramBounds(int number_of_values) const {
    return sketch_.Evaluate([number_of_values](const GenericQuantileSketch& sketch) {
      return sketch.GetHistogramBounds<T>(number_of_values);
    });
  }

 private:
  SketchWrapper<GenericQuantileSketch> sketch_;
};
}  // namespace stats
