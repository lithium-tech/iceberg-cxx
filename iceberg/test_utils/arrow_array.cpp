#include "iceberg/test_utils/arrow_array.h"

namespace iceberg {

template <>
std::shared_ptr<arrow::Array> CreateArray<arrow::TimestampArray>(const OptionalVector<int64_t>& values) {
  arrow::TimestampBuilder builder(std::make_shared<arrow::TimestampType>(), arrow::default_memory_pool());
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

}  // namespace iceberg
