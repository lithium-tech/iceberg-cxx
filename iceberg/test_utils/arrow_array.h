#pragma once

#include <memory>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "iceberg/test_utils/optional_vector.h"

namespace iceberg {

template <typename ArrayBuilder, typename OptVector>
std::shared_ptr<arrow::Array> CreateArray(const OptVector& values) {
  ArrayBuilder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      if (auto status = builder.Append(value.value()); !status.ok()) {
        throw status;
      }
    } else {
      if (auto status = builder.AppendNull(); !status.ok()) {
        throw status;
      }
    }
  }
  auto maybe_array = builder.Finish();
  if (!maybe_array.ok()) {
    throw maybe_array.status();
  }
  return maybe_array.ValueUnsafe();
}

template <>
std::shared_ptr<arrow::Array> CreateArray<arrow::TimestampArray>(const OptionalVector<int64_t>& values);

}  // namespace iceberg
