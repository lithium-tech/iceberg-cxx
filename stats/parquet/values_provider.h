#pragma once

#include "parquet/types.h"
#include "stats/datasketch/dictionary_serializer.h"

namespace stats {

template <typename IndexValue>
class ParquetStringProvider : public IValuesProvider<IndexValue, std::string> {
 public:
  ParquetStringProvider(const parquet::FixedLenByteArray* dictionary_values, uint64_t num_values, int64_t flba_length)
      : dictionary_values_(dictionary_values),
        num_values_(num_values),
        type_(parquet::Type::FIXED_LEN_BYTE_ARRAY),
        flba_length_(flba_length) {}

  ParquetStringProvider(const parquet::ByteArray* dictionary_values, uint64_t num_values)
      : dictionary_values_(dictionary_values), num_values_(num_values), type_(parquet::Type::BYTE_ARRAY) {}

  std::string Get(const IndexValue& index) const override {
    if (type_ == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
      auto* values = static_cast<const parquet::FixedLenByteArray*>(dictionary_values_);
      return std::string(reinterpret_cast<const char*>(values[index].ptr), flba_length_);
    } else {
      auto* values = static_cast<const parquet::ByteArray*>(dictionary_values_);
      return std::string(reinterpret_cast<const char*>(values[index].ptr), values[index].len);
    }
  }

 private:
  const void* dictionary_values_;
  uint64_t num_values_;

  parquet::Type::type type_;
  int64_t flba_length_ = 0;
};

template <typename IndexValue>
class ParquetInt64Provider : public IValuesProvider<IndexValue, int64_t> {
 public:
  ParquetInt64Provider(const int32_t* dictionary_values, uint64_t num_values)
      : dictionary_values_(dictionary_values), num_values_(num_values), type_(parquet::Type::INT32) {}

  ParquetInt64Provider(const int64_t* dictionary_values, uint64_t num_values)
      : dictionary_values_(dictionary_values), num_values_(num_values), type_(parquet::Type::INT64) {}

  // TODO(gmusya): support ByteArray and FixedLenByteArray

  int64_t Get(const IndexValue& index) const override {
    if (type_ == parquet::Type::INT32) {
      auto* values = static_cast<const int32_t*>(dictionary_values_);
      return values[index];
    } else {
      auto* values = static_cast<const int64_t*>(dictionary_values_);
      return values[index];
    }
  }

 private:
  const void* dictionary_values_;
  uint64_t num_values_;

  parquet::Type::type type_;
};

}  // namespace stats