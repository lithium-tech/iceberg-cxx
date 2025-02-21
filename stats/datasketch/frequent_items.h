#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "frequent_items_sketch.hpp"
#include "stats/types.h"

namespace stats {

template <typename T>
class FrequentItemsSketch {
 public:
  static constexpr uint8_t kDefaultLgMaxMapSize = 4;

  explicit FrequentItemsSketch(uint8_t lg_max_map_size = kDefaultLgMaxMapSize) : sketch_(kDefaultLgMaxMapSize) {}

  void AppendValue(const T& value) { sketch_.update(value); }

  std::vector<std::pair<T, double>> GetFrequentItems(int number_of_values) const {
    std::vector<std::pair<T, double>> result;
    const auto& freq_items = sketch_.get_frequent_items(datasketches::frequent_items_error_type::NO_FALSE_POSITIVES);
    result.reserve(freq_items.size());
    for (const auto& item : freq_items) {
      const auto& value = item.get_item();
      double estimate = item.get_estimate();
      result.emplace_back(value, estimate);
    }
    return result;
  }

  datasketches::frequent_items_sketch<T> sketch_;
};

class GenericFrequentItemsSketch {
 public:
  using VariantType = std::variant<FrequentItemsSketch<std::string>, FrequentItemsSketch<int64_t>>;

  explicit GenericFrequentItemsSketch(stats::Type type)
      : sketch_(type == stats::Type::kString ? VariantType(FrequentItemsSketch<std::string>())
                                             : VariantType(FrequentItemsSketch<int64_t>())) {}

  template <typename T>
  void AppendValue(const T& value) {
    std::get<FrequentItemsSketch<T>>(sketch_).AppendValue(value);
  }

  void AppendValue(const void* data, uint64_t size) {
    std::get<FrequentItemsSketch<std::string>>(sketch_).AppendValue(
        std::string(reinterpret_cast<const char*>(data), size));
  }

  template <typename T>
  auto GetFrequentItems(int number_of_values) const {
    return std::get<FrequentItemsSketch<T>>(sketch_).GetFrequentItems(number_of_values);
  }

  VariantType sketch_;
};

}  // namespace stats
