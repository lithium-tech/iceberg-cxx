#pragma once

#include <cstdint>

#include "parquet/types.h"
#include "stats/datasketch/frequent_items.h"
#include "stats/parquet/sketch.h"

namespace stats {

class CommonFrequentItemsWrapper {
 public:
  explicit CommonFrequentItemsWrapper(stats::Type type) : sketch_(type) {}

  void SetFLBALength(int64_t length) { return sketch_.SetFLBALength(length); }

  void AppendValues(const void* data, uint64_t num_values, parquet::Type::type type) {
    return sketch_.AppendValues(data, num_values, type);
  }

  template <typename T>
  auto GetFrequentItems(int number_of_values) const {
    return sketch_.Evaluate([number_of_values](const GenericFrequentItemsSketch& sketch) {
      return sketch.GetFrequentItems<T>(number_of_values);
    });
  }

 private:
  SketchWrapper<GenericFrequentItemsSketch> sketch_;
};
}  // namespace stats
