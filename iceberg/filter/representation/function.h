#pragma once

#include <vector>

#include "iceberg/filter/representation/value.h"

namespace iceberg::filter {

enum class FunctionID {
  kLessThan,
  kLessThanOrEqualTo,
  kGreaterThan,
  kGreaterThanOrEqualTo,
  kEqual,
  kNotEqual,

  kAddWithoutChecks,
  kSubtractWithoutChecks,
  kMultiplyWithoutChecks,
  kDivideWithoutChecks,

  kAddWithChecks,
  kSubtractWithChecks,
  kMultiplyWithChecks,
  kDivideWithChecks,
  kModuloWithChecks,

  kLike,
  kILike,
  kNotLike,
  kNotILike,

  kBitwiseAnd,
  kBitwiseOr,
  kXor,
  kBitwiseNot,

  kIsNull,

  kCastInt4,
  kCastInt8,
  kCastTimestamp,
  kCastTimestamptz,
  kCastFloat8,
  kCastDate,

  kSign,
  kCeil,
  kFloor,
  kRound,
  kSqrt,
  kCbrt,
  kExp,
  kLog10,

  kSin,
  kCos,
  kTan,
  kCot,
  kAtan,
  kAtan2,
  kSinh,
  kCosh,
  kTanh,
  kAsin,
  kAcos,

  kExtractTimestamp,
  kDateTrunc,

  kDateDiff,

  kAbsolute,
  kNegative,

  kCharLength,
  kLower,
  kUpper,
  kLTrim,
  kRTrim,
  kBTrim,
  kSubstring,
  kLocate,
  kConcatenate
};

struct FunctionSignature {
  FunctionID function_id;
  ValueType return_type;
  std::vector<ValueType> argument_types;
};

}  // namespace iceberg::filter
