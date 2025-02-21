#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "quantiles_sketch.hpp"
#include "stats/types.h"

namespace stats {

template <typename T>
class QuantileSketch {
 public:
  QuantileSketch() : sketch_() {}

  void AppendValue(const T& value) { sketch_.update(value); }

  std::vector<T> GetHistogramBounds(int number_of_values) const {
    if (number_of_values < 2) {
      throw std::runtime_error("QuantileSketch: number of values is " + std::to_string(number_of_values));
    }
    std::vector<T> result;
    for (int i = 0; i < number_of_values; ++i) {
      result.emplace_back(sketch_.get_quantile(static_cast<double>(i) / static_cast<double>(number_of_values - 1)));
    }
    return result;
  }

  datasketches::quantiles_sketch<T> sketch_;
};

class GenericQuantileSketch {
 public:
  using VariantType = std::variant<QuantileSketch<std::string>, QuantileSketch<int64_t>>;

  explicit GenericQuantileSketch(stats::Type type)
      : sketch_(type == stats::Type::kString ? VariantType(QuantileSketch<std::string>())
                                             : VariantType(QuantileSketch<int64_t>())) {}

  template <typename T>
  void AppendValue(const T& value) {
    std::get<QuantileSketch<T>>(sketch_).AppendValue(value);
  }

  void AppendValue(const void* data, uint64_t size) {
    std::get<QuantileSketch<std::string>>(sketch_).AppendValue(std::string(reinterpret_cast<const char*>(data), size));
  }

  template <typename T>
  auto GetHistogramBounds(int number_of_values) const {
    return std::get<QuantileSketch<T>>(sketch_).GetHistogramBounds(number_of_values);
  }

  VariantType sketch_;
};

}  // namespace stats
