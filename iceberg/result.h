#pragma once

#include "arrow/result.h"
#include "iceberg/common/error.h"

namespace iceberg {

// TODO(gmusya): consider throwing std::runtime_error instead of arrow::Status
inline void Ensure(const arrow::Status& s) { Ensure(s.ok(), s.message()); }

template <typename T>
T& ValueSafe(arrow::Result<T>& value) {
  Ensure(value.ok(), value.status().message());
  return value.ValueUnsafe();
}

template <typename T>
const T& ValueSafe(const arrow::Result<T>& value) {
  Ensure(value.ok(), value.status().message());
  return value.ValueUnsafe();
}

template <typename T>
T ValueSafe(arrow::Result<T>&& value) {
  Ensure(value.ok(), value.status().message());
  return value.MoveValueUnsafe();
}

template <typename T>
T MoveValueSafe(arrow::Result<T>&& value) {
  Ensure(value.ok(), value.status().message());
  return value.MoveValueUnsafe();
}

}  // namespace iceberg
