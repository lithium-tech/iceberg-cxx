#include "iceberg/filter/stats_filter/registry.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <limits>
#include <set>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/value.h"
#include "iceberg/filter/stats_filter/stats.h"

namespace iceberg::filter {

namespace {

using FID = FunctionID;
using VT = ValueType;

template <VT return_type, VT arg_type>
using TypedUnaryEvalutor = MinMaxStats<return_type> (*)(const MinMaxStats<arg_type>&);
using UnaryEvaluator = arrow::Result<GenericStats> (*)(const GenericStats&);

template <VT return_type, VT arg_type>
using TypedBinaryEvalutor = MinMaxStats<return_type> (*)(const MinMaxStats<arg_type>&, const MinMaxStats<arg_type>&);
using BinaryEvaluator = arrow::Result<GenericStats> (*)(const GenericStats&, const GenericStats&);

// Usage assumptions:
// * cmp(x, y) returns true if (x, y) matches condition
// * cmp(x1, y) == cmp(x2, y) && x1 <= x <= x2  =>  cmp(x, y) == cmp(x1, y)
// * cmp(x, y1) == cmp(x, y2) && y1 <= y <= y2  =>  cmp(x, y) == cmp(x, y1)
template <VT value_type, typename Comparator>
MinMaxStats<VT::kBool> EvaluateMonotoneComparison(const MinMaxStats<value_type>& lhs,
                                                  const MinMaxStats<value_type>& rhs) {
  Comparator cmp;
  bool everything_true = true;
  bool everything_false = true;
  for (const auto& val1 : {lhs.min, lhs.max}) {
    for (const auto& val2 : {rhs.min, rhs.max}) {
      if (cmp(val1, val2)) {
        everything_false = false;
      } else {
        everything_true = false;
      }
    }
  }
  if (everything_false) {
    return kBoolStatsFalse;
  }
  if (everything_true) {
    return kBoolStatsTrue;
  }
  return kBoolStatsUnknown;
}

template <VT value_type>
MinMaxStats<VT::kBool> EvaluateLess(const MinMaxStats<value_type>& lhs, const MinMaxStats<value_type>& rhs) {
  return EvaluateMonotoneComparison<value_type, std::less<PhysicalType<value_type>>>(lhs, rhs);
}

template <VT value_type>
MinMaxStats<VT::kBool> EvaluateGreater(const MinMaxStats<value_type>& lhs, const MinMaxStats<value_type>& rhs) {
  return EvaluateMonotoneComparison<value_type, std::greater<PhysicalType<value_type>>>(lhs, rhs);
}

template <VT value_type>
MinMaxStats<VT::kBool> EvaluateLessThanOrEqualTo(const MinMaxStats<value_type>& lhs,
                                                 const MinMaxStats<value_type>& rhs) {
  return EvaluateMonotoneComparison<value_type, std::less_equal<PhysicalType<value_type>>>(lhs, rhs);
}

template <VT value_type>
MinMaxStats<VT::kBool> EvaluateGreaterThanOrEqualTo(const MinMaxStats<value_type>& lhs,
                                                    const MinMaxStats<value_type>& rhs) {
  return EvaluateMonotoneComparison<value_type, std::greater_equal<PhysicalType<value_type>>>(lhs, rhs);
}

template <VT value_type>
MinMaxStats<VT::kBool> EvaluateEqual(const MinMaxStats<value_type>& lhs, const MinMaxStats<value_type>& rhs) {
  auto res_lteq = EvaluateMonotoneComparison<value_type, std::less_equal<PhysicalType<value_type>>>(lhs, rhs);
  auto res_gteq = EvaluateMonotoneComparison<value_type, std::greater_equal<PhysicalType<value_type>>>(lhs, rhs);

  return res_lteq && res_gteq;
}

template <VT value_type>
MinMaxStats<VT::kBool> EvaluateNotEqual(const MinMaxStats<value_type>& lhs, const MinMaxStats<value_type>& rhs) {
  return !(EvaluateEqual(lhs, rhs));
}

template <VT return_type, VT arg_type, TypedBinaryEvalutor<return_type, arg_type> binary_evaluator>
arrow::Result<GenericStats> EvaluateBinary(const GenericStats& lhs, const GenericStats& rhs) {
  if (lhs.min_max.GetValueType() != arg_type) {
    return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": incorrect lhs type ",
                                         ValueTypeToString(lhs.min_max.GetValueType()), ", expected ",
                                         ValueTypeToString(arg_type));
  }
  if (rhs.min_max.GetValueType() != arg_type) {
    return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": incorrect rhs type ",
                                         ValueTypeToString(lhs.min_max.GetValueType()), ", expected ",
                                         ValueTypeToString(arg_type));
  }
  auto min_max =
      GenericMinMaxStats::Make<return_type>(binary_evaluator(lhs.min_max.Get<arg_type>(), rhs.min_max.Get<arg_type>()));

  bool contains_non_null = lhs.count.contains_non_null || rhs.count.contains_non_null;
  bool contains_null = lhs.count.contains_null || rhs.count.contains_null;

  return GenericStats(std::move(min_max),
                      CountStats{.contains_null = contains_null, .contains_non_null = contains_non_null});
}

MinMaxStats<VT::kInt4> EvaluateCastInt2Int4(const MinMaxStats<VT::kInt2>& arg) {
  return MinMaxStats<VT::kInt4>{.min = arg.min, .max = arg.max};
}

MinMaxStats<VT::kInt8> EvaluateCastInt2Int8(const MinMaxStats<VT::kInt2>& arg) {
  return MinMaxStats<VT::kInt8>{.min = arg.min, .max = arg.max};
}

MinMaxStats<VT::kInt8> EvaluateCastInt4Int8(const MinMaxStats<VT::kInt4>& arg) {
  return MinMaxStats<VT::kInt8>{.min = arg.min, .max = arg.max};
}

MinMaxStats<VT::kFloat8> EvaluateCastFloat4Float8(const MinMaxStats<VT::kFloat4>& arg) {
  return MinMaxStats<VT::kFloat8>{.min = arg.min, .max = arg.max};
}

MinMaxStats<VT::kTimestamp> EvaluateCastDateTimestamp(const MinMaxStats<VT::kDate>& arg) {
  constexpr int64_t kMicrosInDay = static_cast<int64_t>(24) * 60 * 60 * 1000 * 1000;
  constexpr int32_t kMaxSupportedDate = 1'000'000;

  static_assert(kMaxSupportedDate * kMicrosInDay >= 1);  // does not overflow

  if (abs(arg.min) <= kMaxSupportedDate && abs(arg.max) <= kMaxSupportedDate) {
    return MinMaxStats<VT::kTimestamp>{.min = arg.min * kMicrosInDay, .max = arg.max * kMicrosInDay};
  }

  return MinMaxStats<VT::kTimestamp>{.min = std::numeric_limits<PhysicalType<VT::kTimestamp>>::min(),
                                     .max = std::numeric_limits<PhysicalType<VT::kTimestamp>>::max()};
}

MinMaxStats<VT::kTimestamp> EvaluateCastTimestamptzTimestamp(const MinMaxStats<VT::kTimestamptz>& arg,
                                                             int64_t timestamp_to_timestamptz_shift_us) {
  constexpr int64_t kMaxSupportedTimestamp = 1e18;

  static_assert(kMaxSupportedTimestamp + kMaxSupportedTimestamp);

  if (abs(arg.min) <= kMaxSupportedTimestamp && abs(arg.max) <= kMaxSupportedTimestamp &&
      abs(timestamp_to_timestamptz_shift_us) <= kMaxSupportedTimestamp) {
    return MinMaxStats<VT::kTimestamp>{.min = arg.min + timestamp_to_timestamptz_shift_us,
                                       .max = arg.max + timestamp_to_timestamptz_shift_us};
  }

  return MinMaxStats<VT::kTimestamp>{.min = std::numeric_limits<PhysicalType<VT::kTimestamp>>::min(),
                                     .max = std::numeric_limits<PhysicalType<VT::kTimestamp>>::max()};
}

arrow::Result<GenericStats> EvaluateIsNull(const GenericStats& stats) {
  const auto& arg_count_stats = stats.count;

  bool has_true = arg_count_stats.contains_null;
  bool has_false = arg_count_stats.contains_non_null;

  CountStats result_count_stats{.contains_null = false, .contains_non_null = true};
  auto minmax =
      GenericMinMaxStats::Make<ValueType::kBool>(MinMaxStats<ValueType::kBool>{.min = !has_false, .max = has_true});
  return GenericStats(std::move(minmax), result_count_stats);
}

template <VT return_type, VT arg_type, TypedUnaryEvalutor<return_type, arg_type> unary_evaluator>
arrow::Result<GenericStats> EvaluateUnary(const GenericStats& arg) {
  if (arg.min_max.GetValueType() != arg_type) {
    return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": incorrect arg type ",
                                         ValueTypeToString(arg.min_max.GetValueType()), ", expected ",
                                         ValueTypeToString(arg_type));
  }

  auto min_max = GenericMinMaxStats::Make<return_type>(unary_evaluator(arg.min_max.Get<arg_type>()));

  return GenericStats(std::move(min_max), arg.count);
}

struct BinaryEvalutorRegisterEntry {
  FID id;
  VT lhs_type;
  VT rhs_type;

  BinaryEvaluator binary_evaluator;
};

struct UnaryEvalutorRegisterEntry {
  FID id;
  VT arg_type;

  UnaryEvaluator unary_evaluator;
};

#define REGISTER_FOR_ALL_TYPES(MACRO)                                                                             \
  MACRO(VT::kBool), MACRO(VT::kInt2), MACRO(VT::kInt4), MACRO(VT::kInt8), MACRO(VT::kFloat4), MACRO(VT::kFloat8), \
      MACRO(VT::kNumeric), MACRO(VT::kString), MACRO(VT::kDate), MACRO(VT::kTimestamp), MACRO(VT::kTimestamptz),  \
      MACRO(VT::kTime), MACRO(VT::kInterval)

#define REGISTER_BINARY_EVALUATOR(fid, return_type, arg_type, func) \
  { fid, arg_type, arg_type, EvaluateBinary<return_type, arg_type, func> }

#define REGISTER_COMPARISON(arg_type)                                                                           \
  REGISTER_BINARY_EVALUATOR(FID::kLessThan, VT::kBool, arg_type, EvaluateLess),                                 \
      REGISTER_BINARY_EVALUATOR(FID::kGreaterThan, VT::kBool, arg_type, EvaluateGreater),                       \
      REGISTER_BINARY_EVALUATOR(FID::kLessThanOrEqualTo, VT::kBool, arg_type, EvaluateLessThanOrEqualTo),       \
      REGISTER_BINARY_EVALUATOR(FID::kGreaterThanOrEqualTo, VT::kBool, arg_type, EvaluateGreaterThanOrEqualTo), \
      REGISTER_BINARY_EVALUATOR(FID::kEqual, VT::kBool, arg_type, EvaluateEqual),                               \
      REGISTER_BINARY_EVALUATOR(FID::kNotEqual, VT::kBool, arg_type, EvaluateNotEqual)

// TODO(gmusya): support numeric comparison
constexpr BinaryEvalutorRegisterEntry kBinaryEvaluators[] = {
    REGISTER_COMPARISON(VT::kBool),        REGISTER_COMPARISON(VT::kFloat4), REGISTER_COMPARISON(VT::kFloat8),
    REGISTER_COMPARISON(VT::kInt2),        REGISTER_COMPARISON(VT::kInt4),   REGISTER_COMPARISON(VT::kInt8),
    REGISTER_COMPARISON(VT::kDate),        REGISTER_COMPARISON(VT::kTime),   REGISTER_COMPARISON(VT::kTimestamp),
    REGISTER_COMPARISON(VT::kTimestamptz), REGISTER_COMPARISON(VT::kString)};

#undef REGISTER_COMPARISON
#undef REGISTER_BINARY_EVALUATOR

#define REGISTER_UNARY_EVALUATOR(fid, return_type, arg_type, func) \
  { fid, arg_type, EvaluateUnary<return_type, arg_type, func> }

#define REGISTER_IS_NULL(arg_type) \
  { FID::kIsNull, arg_type, EvaluateIsNull }

constexpr UnaryEvalutorRegisterEntry kUnaryEvaluators[] = {
    REGISTER_UNARY_EVALUATOR(FID::kCastInt4, VT::kInt4, VT::kInt2, EvaluateCastInt2Int4),
    REGISTER_UNARY_EVALUATOR(FID::kCastInt8, VT::kInt8, VT::kInt2, EvaluateCastInt2Int8),
    REGISTER_UNARY_EVALUATOR(FID::kCastInt8, VT::kInt8, VT::kInt4, EvaluateCastInt4Int8),
    REGISTER_UNARY_EVALUATOR(FID::kCastFloat8, VT::kFloat8, VT::kFloat4, EvaluateCastFloat4Float8),
    REGISTER_FOR_ALL_TYPES(REGISTER_IS_NULL)};

#undef REGISTER_IS_NOT_NULL
#undef REGISTER_IS_NULL
#undef REGISTER_IS_NULL_EVALUATOR
#undef REGISTER_UNARY_EVALUATOR

bool AreCastsBeforeInvokeAllowed(FunctionID fid) {
  switch (fid) {
    case FID::kLessThan:
    case FID::kLessThanOrEqualTo:
    case FID::kGreaterThan:
    case FID::kGreaterThanOrEqualTo:
    case FID::kNotEqual:
    case FID::kEqual:
      return true;
    default:
      return false;
  }
  return false;
}

}  // namespace

arrow::Result<std::vector<GenericStats>> Registry::CastToSameTypeForComparison(std::vector<GenericStats> args) const {
  std::vector<ValueType> value_types;
  for (const auto& arg : args) {
    value_types.emplace_back(arg.min_max.GetValueType());
  }
  std::set<ValueType> unique_value_types(value_types.begin(), value_types.end());
  if (unique_value_types.size() == 1) {
    return args;
  }

  if (unique_value_types == std::set<ValueType>{VT::kInt2, VT::kInt4}) {
    for (auto& arg : args) {
      if (arg.min_max.GetValueType() == VT::kInt2) {
        ARROW_ASSIGN_OR_RAISE(arg, (EvaluateUnary<VT::kInt4, VT::kInt2, EvaluateCastInt2Int4>(std::move(arg))));
      }
    }
    return args;
  }

  if ((unique_value_types == std::set<ValueType>{VT::kInt2, VT::kInt4, VT::kInt8}) ||
      (unique_value_types == std::set<ValueType>{VT::kInt4, VT::kInt8}) ||
      (unique_value_types == std::set<ValueType>{VT::kInt2, VT::kInt8})) {
    for (auto& arg : args) {
      if (arg.min_max.GetValueType() == VT::kInt2) {
        ARROW_ASSIGN_OR_RAISE(arg, (EvaluateUnary<VT::kInt8, VT::kInt2, EvaluateCastInt2Int8>(std::move(arg))));
      } else if (arg.min_max.GetValueType() == VT::kInt4) {
        ARROW_ASSIGN_OR_RAISE(arg, (EvaluateUnary<VT::kInt8, VT::kInt4, EvaluateCastInt4Int8>(std::move(arg))));
      }
    }
    return args;
  }

  if (unique_value_types == std::set<ValueType>{VT::kFloat4, VT::kFloat8}) {
    for (auto& arg : args) {
      if (arg.min_max.GetValueType() == VT::kFloat4) {
        ARROW_ASSIGN_OR_RAISE(arg, (EvaluateUnary<VT::kFloat8, VT::kFloat4, EvaluateCastFloat4Float8>(std::move(arg))));
      }
    }
    return args;
  }

  // cast all to timestamp
  if (unique_value_types == std::set<ValueType>{VT::kTimestamp, VT::kTimestamptz} ||
      unique_value_types == std::set<ValueType>{VT::kTimestamp, VT::kDate} ||
      unique_value_types == std::set<ValueType>{VT::kTimestamptz, VT::kDate}) {
    for (auto& arg : args) {
      if (arg.min_max.GetValueType() == VT::kDate) {
        ARROW_ASSIGN_OR_RAISE(arg,
                              (EvaluateUnary<VT::kTimestamp, VT::kDate, EvaluateCastDateTimestamp>(std::move(arg))));
      } else if (arg.min_max.GetValueType() == VT::kTimestamptz) {
        if (!settings_.timestamp_to_timestamptz_shift_us) {
          return arrow::Status::ExecutionError(
              __PRETTY_FUNCTION__,
              ": unsupported cast from timestamptz to timestamp because timestamp_to_timestamptz_shift_us is not set");
        }
        MinMaxStats<VT::kTimestamp> timestamp_stats = EvaluateCastTimestamptzTimestamp(
            arg.min_max.Get<VT::kTimestamptz>(), *settings_.timestamp_to_timestamptz_shift_us);
        arg = GenericStats(GenericMinMaxStats::Make<VT::kTimestamp>(timestamp_stats), arg.count);
      }
    }

    return args;
  }

  return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": unsupported cast");
}

arrow::Result<GenericStats> Registry::Evaluate(FID function_id, std::vector<GenericStats> args) const {
  if (args.size() == 2) {
    if (AreCastsBeforeInvokeAllowed(function_id)) {
      ARROW_ASSIGN_OR_RAISE(args, CastToSameTypeForComparison(std::move(args)));
    }
    const auto& lhs = args.at(0);
    const auto& rhs = args.at(1);
    for (const auto& [id, lhs_type, rhs_type, evaluator] : kBinaryEvaluators) {
      if (id == function_id && lhs.min_max.GetValueType() == lhs_type && rhs.min_max.GetValueType() == rhs_type) {
        return evaluator(lhs, rhs);
      }
    }
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__, ": number of arguments = 2, unexpected function id ",
                                         std::to_string(static_cast<int>(function_id)));
  }
  if (args.size() == 1) {
    const auto& arg = args.at(0);
    for (const auto& [id, arg_type, evaluator] : kUnaryEvaluators) {
      if (id == function_id && arg.min_max.GetValueType() == arg_type) {
        return evaluator(arg);
      }
    }
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__, ": number of arguments = 1, unexpected function id ",
                                         std::to_string(static_cast<int>(function_id)));
  }
  return arrow::Status::NotImplemented(__PRETTY_FUNCTION__, ": unexpected number of arguments (", args.size(), ")");
}

}  // namespace iceberg::filter
