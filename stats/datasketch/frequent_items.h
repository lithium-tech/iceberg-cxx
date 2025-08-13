#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "frequent_items_sketch.hpp"
#include "stats/datasketch/dictionary_serializer.h"
#include "stats/types.h"

namespace stats {

template <typename T>
class FrequentItemsSketch {
 public:
  using ValueType = T;

  static constexpr uint8_t kDefaultLgMaxMapSize = 4;

  explicit FrequentItemsSketch(uint8_t lg_max_map_size = kDefaultLgMaxMapSize) : sketch_(kDefaultLgMaxMapSize) {}

  explicit FrequentItemsSketch(const datasketches::frequent_items_sketch<T>& other) : sketch_(other) {}
  explicit FrequentItemsSketch(datasketches::frequent_items_sketch<T>&& other) : sketch_(std::move(other)) {}

  void AppendValue(const T& value, int64_t weight = 1) { sketch_.update(value, weight); }

  std::vector<std::pair<T, double>> GetFrequentItems() const {
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

  bool Empty() const { return sketch_.is_empty(); }

  datasketches::frequent_items_sketch<T>& GetSketch() { return sketch_; }
  const datasketches::frequent_items_sketch<T>& GetSketch() const { return sketch_; }

 private:
  datasketches::frequent_items_sketch<T> sketch_;
};

class GenericFrequentItemsSketch {
 public:
  using VariantType = std::variant<FrequentItemsSketch<std::string>, FrequentItemsSketch<int64_t>>;

  explicit GenericFrequentItemsSketch(stats::Type type)
      : sketch_(type == stats::Type::kString ? VariantType(FrequentItemsSketch<std::string>())
                                             : VariantType(FrequentItemsSketch<int64_t>())) {}

  explicit GenericFrequentItemsSketch(const VariantType& other) : sketch_(other) {}
  explicit GenericFrequentItemsSketch(VariantType&& other) : sketch_(std::move(other)) {}

  template <typename T>
  void AppendValue(const T& value) {
    std::get<FrequentItemsSketch<T>>(sketch_).AppendValue(value);
  }

  void AppendValue(const void* data, uint64_t size) {
    std::get<FrequentItemsSketch<std::string>>(sketch_).AppendValue(
        std::string(reinterpret_cast<const char*>(data), size));
  }

  template <typename T>
  void AppendWeightedValue(const T& value, int64_t weight) {
    std::get<FrequentItemsSketch<T>>(sketch_).AppendValue(value, weight);
  }

  void AppendWeightedValue(const void* data, uint64_t size, int64_t weight) {
    std::get<FrequentItemsSketch<std::string>>(sketch_).AppendValue(
        std::string(reinterpret_cast<const char*>(data), size), weight);
  }

  template <typename T>
  auto GetFrequentItems() const {
    return std::get<FrequentItemsSketch<T>>(sketch_).GetFrequentItems();
  }

  bool Empty() const {
    return std::visit([](auto&& arg) { return arg.Empty(); }, sketch_);
  }

  stats::Type Type() const {
    if (std::holds_alternative<FrequentItemsSketch<std::string>>(sketch_)) {
      return stats::Type::kString;
    } else {
      return stats::Type::kInt64;
    }
  }

  template <typename DictionaryValue>
  GenericFrequentItemsSketch FromDictionary(const IValuesProvider<int64_t, DictionaryValue>& dictionary_values) const {
    iceberg::Ensure(std::holds_alternative<FrequentItemsSketch<int64_t>>(sketch_),
                    std::string(__PRETTY_FUNCTION__) +
                        ": trying to resolve dictionary in sketch, but sketch does not store integers");

    const auto& typed_sketch = std::get<FrequentItemsSketch<int64_t>>(sketch_).GetSketch();
    auto result = ResolveDictionary<int64_t, DictionaryValue>(typed_sketch, dictionary_values);

    return GenericFrequentItemsSketch(FrequentItemsSketch<DictionaryValue>(std::move(result)));
  }

  void Merge(const GenericFrequentItemsSketch& other) {
    std::visit(
        [&]<typename ValueType>(FrequentItemsSketch<ValueType>& sketch) {
          iceberg::Ensure(std::holds_alternative<FrequentItemsSketch<ValueType>>(other.sketch_),
                          std::string(__PRETTY_FUNCTION__) + ": merging sketches with different types is impossible");

          const auto& raw_sketch = std::get<FrequentItemsSketch<ValueType>>(other.sketch_).GetSketch();

          sketch.GetSketch().merge(raw_sketch);
        },
        sketch_);
  }

 private:
  VariantType sketch_;
};

}  // namespace stats
