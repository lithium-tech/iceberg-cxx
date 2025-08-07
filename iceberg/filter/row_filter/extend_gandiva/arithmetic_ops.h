#pragma once

#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "gandiva/gdv_function_stubs.h"

namespace iceberg {

template <typename FromType, typename ToType>
ToType Cast(FromType value) {
  return static_cast<ToType>(value);
}

template <typename T>
T AddOverflow(int64_t context, T lhs, T rhs) {
  T res;
  if (__builtin_add_overflow(lhs, rhs, &res)) {
    gdv_fn_context_set_error_msg(context, "Overflow");
    return 0;
  }
  return res;
}

template <typename T>
T SubOverflow(int64_t context, T lhs, T rhs) {
  T res;
  if (__builtin_sub_overflow(lhs, rhs, &res)) {
    gdv_fn_context_set_error_msg(context, "Overflow");
    return 0;
  }
  return res;
}

template <typename T>
T MulOverflow(int64_t context, T lhs, T rhs) {
  T res;
  if (__builtin_mul_overflow(lhs, rhs, &res)) {
    gdv_fn_context_set_error_msg(context, "Overflow");
    return 0;
  }
  return res;
}

template <typename T>
T DivSafe(int64_t context, T lhs, T rhs) {
  if (rhs == 0) {
    gdv_fn_context_set_error_msg(context, "Division by zero");
    return 0;
  }
  if constexpr (std::is_signed_v<T>) {
    if (lhs == std::numeric_limits<T>::min() && rhs == -1) {
      gdv_fn_context_set_error_msg(context, "Overflow");
      return 0;
    }
  }
  return lhs / rhs;
}

template <typename T>
T ModSafe(int64_t context, T lhs, T rhs) {
  if (rhs == 0) {
    gdv_fn_context_set_error_msg(context, "Division by zero");
    return 0;
  }
  if constexpr (std::is_signed_v<T>) {
    if (lhs == std::numeric_limits<T>::min() && rhs == -1) {
      return 0;
    }
  }
  return lhs % rhs;
}

template <typename T1, typename T2 = T1>
bool LessThan(T1 lhs, T2 rhs) {
  return lhs < rhs;
}

template <typename T1, typename T2 = T1>
bool LessThanOrEqualTo(T1 lhs, T2 rhs) {
  return lhs <= rhs;
}

template <typename T1, typename T2 = T1>
bool GreaterThan(T1 lhs, T2 rhs) {
  return lhs > rhs;
}

template <typename T1, typename T2 = T1>
bool GreaterThanOrEqualTo(T1 lhs, T2 rhs) {
  return lhs >= rhs;
}

template <typename T1, typename T2 = T1>
bool Equal(T1 lhs, T2 rhs) {
  return lhs == rhs;
}

template <typename T1, typename T2 = T1>
bool NotEqual(T1 lhs, T2 rhs) {
  return lhs != rhs;
}

inline double BankersRound(double x) { return std::rint(x); }

}  // namespace iceberg
