#pragma once

#include <type_traits>
#include <utility>

#include "arrow/status.h"

#define ASSERT_OK(expr)                                                       \
  static_assert(std::is_same_v<std::decay_t<decltype(expr)>, arrow::Status>); \
  ASSERT_EQ(expr, arrow::Status::OK())

#define ASSIGN_OR_FAIL_IMPL(result_name, lhs, rexpr) \
  auto&& result_name = (rexpr);                      \
  ASSERT_OK(result_name.status());                   \
  lhs = std::move(result_name).ValueUnsafe();

#define ASSIGN_OR_FAIL_NAME(x, y) ARROW_CONCAT(x, y)

#define ASSIGN_OR_FAIL(lhs, rexpr) ASSIGN_OR_FAIL_IMPL(ASSIGN_OR_FAIL_NAME(_error_or_value, __COUNTER__), lhs, rexpr);
