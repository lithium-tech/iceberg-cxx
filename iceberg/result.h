#pragma once

#include "arrow/result.h"

namespace iceberg {

// TODO(gmusya): consider throwing std::runtime_error instead of arrow::Status
inline void Ensure(const arrow::Status& s) {
  if (!s.ok()) {
    throw s;
  }
}

template <typename T>
T& ValueSafe(arrow::Result<T>& value) {
  if (!value.ok()) {
    throw value.status();
  }
  return value.ValueUnsafe();
}

template <typename T>
const T& ValueSafe(const arrow::Result<T>& value) {
  if (!value.ok()) {
    throw value.status();
  }
  return value.ValueUnsafe();
}

template <typename T>
T ValueSafe(arrow::Result<T>&& value) {
  if (!value.ok()) {
    throw value.status();
  }
  return value.MoveValueUnsafe();
}

template <typename T>
T MoveValueSafe(arrow::Result<T>&& value) {
  if (!value.ok()) {
    throw value.status();
  }
  return value.MoveValueUnsafe();
}

}  // namespace iceberg
