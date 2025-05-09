#pragma once

#include <cstdint>
#include <iostream>
#include <utility>

#include "parquet/types.h"

namespace stats {

template <typename Sketch>
class SketchUpdater {
 public:
  using SketchType = Sketch;

  explicit SketchUpdater(Sketch& s) : sketch_(s) {}

  void SetFLBALength(int64_t length) { flba_length_ = length; }

  void AppendValues(const void* data, uint64_t num_values, parquet::Type::type type) {
    switch (type) {
      case parquet::Type::INT32: {
        const int32_t* values = reinterpret_cast<const int32_t*>(data);
        for (uint64_t i = 0; i < num_values; ++i) {
          sketch_.AppendValue(static_cast<int64_t>(values[i]));
        }
        break;
      }
      case parquet::Type::INT64: {
        const int64_t* values = reinterpret_cast<const int64_t*>(data);
        for (uint64_t i = 0; i < num_values; ++i) {
          sketch_.AppendValue(values[i]);
        }
        break;
      }
      case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        const parquet::FixedLenByteArray* values = reinterpret_cast<const parquet::FixedLenByteArray*>(data);
        for (uint64_t i = 0; i < num_values; ++i) {
          sketch_.AppendValue(values[i].ptr, flba_length_);
        }
        break;
      }
      case parquet::Type::BYTE_ARRAY: {
        const parquet::ByteArray* values = reinterpret_cast<const parquet::ByteArray*>(data);
        for (uint64_t i = 0; i < num_values; ++i) {
          sketch_.AppendValue(values[i].ptr, values[i].len);
        }
        break;
      }
      default:
        std::cerr << "Unexpected data type " << type << " ignored " << std::endl;
    }
  }

 private:
  Sketch& sketch_;
  int64_t flba_length_;
};

}  // namespace stats
