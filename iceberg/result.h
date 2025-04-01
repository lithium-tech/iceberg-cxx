#include "arrow/result.h"

namespace iceberg {

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
