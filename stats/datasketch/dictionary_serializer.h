#pragma once

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "frequent_items_sketch.hpp"
#include "iceberg/common/error.h"
#include "quantiles_sketch.hpp"
#include "serde.hpp"

namespace stats {

template <typename IndexValue, typename DictionaryValue>
class IValuesProvider {
 public:
  virtual DictionaryValue Get(const IndexValue& index) const = 0;

  virtual ~IValuesProvider() = default;
};

template <typename IndexValue, typename DictionaryValue>
class SpanValuesProvider : public IValuesProvider<IndexValue, DictionaryValue> {
 public:
  explicit SpanValuesProvider(std::span<DictionaryValue> values) : values_(values) {}

  DictionaryValue Get(const IndexValue& index) const override {
    Ensure(index >= 0 && index < values_.size(), std::string(__PRETTY_FUNCTION__) +
                                                     ": out of bounds (index = " + std::to_string(index) +
                                                     ", size = " + std::to_string(values_.size()));
    return values_[index];
  }

 private:
  std::span<DictionaryValue> values_;
};

template <typename IndexValue, typename DictionaryValue>
class Serializer {
 public:
  explicit Serializer(const IValuesProvider<IndexValue, DictionaryValue>& dictionary_resolver)
      : dictionary_resolver_(dictionary_resolver) {}

  size_t serialize(void* ptr, size_t capacity, const IndexValue* items, unsigned num) const {
    std::vector<DictionaryValue> uncompressed_data(num);
    for (unsigned i = 0; i < num; ++i) {
      uncompressed_data[i] = dictionary_resolver_.Get(items[i]);
    }
    return typed_serde_.serialize(ptr, capacity, uncompressed_data.data(), num);
  }

  size_t size_of_item(const IndexValue& item) const {
    return typed_serde_.size_of_item(dictionary_resolver_.Get(item));
  }

 private:
  const IValuesProvider<IndexValue, DictionaryValue>& dictionary_resolver_;
  datasketches::serde<DictionaryValue> typed_serde_;
};

template <typename IndexValue, typename DictionaryValue>
auto ResolveDictionary(const datasketches::frequent_items_sketch<IndexValue>& sketch,
                       const IValuesProvider<IndexValue, DictionaryValue>& values_provider) {
  Serializer<IndexValue, DictionaryValue> s(values_provider);
  auto serialized_bytes = sketch.serialize(0, s);

  return datasketches::frequent_items_sketch<DictionaryValue>::deserialize(serialized_bytes.data(),
                                                                           serialized_bytes.size());
}

template <typename IndexValue, typename DictionaryValue>
auto ResolveDictionary(const datasketches::quantiles_sketch<IndexValue>& sketch,
                       const IValuesProvider<IndexValue, DictionaryValue>& values_provider) {
  Serializer<IndexValue, DictionaryValue> s(values_provider);
  auto serialized_bytes = sketch.serialize(0, s);

  return datasketches::quantiles_sketch<DictionaryValue>::deserialize(serialized_bytes.data(), serialized_bytes.size());
}

}  // namespace stats
