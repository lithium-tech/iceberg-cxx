#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "quantiles_sketch.hpp"
#include "stats/datasketch/dictionary_serializer.h"
#include "stats/types.h"

namespace stats {

template <typename T>
class QuantileSketch {
 public:
  QuantileSketch() : sketch_() {}

  explicit QuantileSketch(const datasketches::quantiles_sketch<T>& other) : sketch_(other) {}
  explicit QuantileSketch(datasketches::quantiles_sketch<T>&& other) : sketch_(std::move(other)) {}

  void AppendValue(const T& value) { sketch_.update(value); }

  std::vector<T> GetHistogramBounds(int number_of_values) const {
    iceberg::Ensure(number_of_values >= 2, "QuantileSketch: number of values is " + std::to_string(number_of_values));
    std::vector<T> result;
    for (int i = 0; i < number_of_values; ++i) {
      result.emplace_back(sketch_.get_quantile(static_cast<double>(i) / static_cast<double>(number_of_values - 1)));
    }
    return result;
  }

  bool Empty() const { return sketch_.is_empty(); }

  datasketches::quantiles_sketch<T>& GetSketch() { return sketch_; }
  const datasketches::quantiles_sketch<T>& GetSketch() const { return sketch_; }

 private:
  datasketches::quantiles_sketch<T> sketch_;
};

class GenericQuantileSketch {
 public:
  using VariantType = std::variant<QuantileSketch<std::string>, QuantileSketch<int64_t>>;

  explicit GenericQuantileSketch(stats::Type type)
      : sketch_(type == stats::Type::kString ? VariantType(QuantileSketch<std::string>())
                                             : VariantType(QuantileSketch<int64_t>())) {}

  explicit GenericQuantileSketch(const VariantType& other) : sketch_(other) {}
  explicit GenericQuantileSketch(VariantType&& other) : sketch_(std::move(other)) {}

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

  bool Empty() const {
    return std::visit([](auto&& arg) { return arg.Empty(); }, sketch_);
  }

  stats::Type Type() const {
    if (std::holds_alternative<QuantileSketch<std::string>>(sketch_)) {
      return stats::Type::kString;
    } else {
      return stats::Type::kInt64;
    }
  }

  template <typename DictionaryValue>
  GenericQuantileSketch FromDictionary(const IValuesProvider<int64_t, DictionaryValue>& dictionary_values) const {
    iceberg::Ensure(std::holds_alternative<QuantileSketch<int64_t>>(sketch_),
                    std::string(__PRETTY_FUNCTION__) +
                        ": trying to resolve dictionary in sketch, but sketch does not store integers");

    const auto& typed_sketch = std::get<QuantileSketch<int64_t>>(sketch_).GetSketch();
    auto result = ResolveDictionary<int64_t, DictionaryValue>(typed_sketch, dictionary_values);

    return GenericQuantileSketch(QuantileSketch<DictionaryValue>(std::move(result)));
  }

  void Merge(const GenericQuantileSketch& other) {
    std::visit(
        [&]<typename ValueType>(QuantileSketch<ValueType>& sketch) {
          iceberg::Ensure(std::holds_alternative<QuantileSketch<ValueType>>(other.sketch_),
                          std::string(__PRETTY_FUNCTION__) + ": merging sketches with different types is impossible");
          const auto& raw_sketch = std::get<QuantileSketch<ValueType>>(other.sketch_).GetSketch();

          sketch.GetSketch().merge(raw_sketch);
        },
        sketch_);
  }

 private:
  VariantType sketch_;
};

}  // namespace stats
