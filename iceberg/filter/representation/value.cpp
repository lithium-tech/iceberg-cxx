#include "iceberg/filter/representation/value.h"

namespace iceberg::filter {

std::string ValueTypeToString(ValueType value_type) {
  switch (value_type) {
    case ValueType::kBool:
      return "bool";
    case ValueType::kInt2:
      return "int2";
    case ValueType::kInt4:
      return "int4";
    case ValueType::kInt8:
      return "int8";
    case ValueType::kFloat4:
      return "float4";
    case ValueType::kFloat8:
      return "float8";
    case ValueType::kNumeric:
      return "numeric";
    case ValueType::kString:
      return "string";
    case ValueType::kDate:
      return "date";
    case ValueType::kTime:
      return "time";
    case ValueType::kTimestamp:
      return "timestamp";
    case ValueType::kTimestamptz:
      return "timestamptz";
    case ValueType::kInterval:
      return "interval";
  }
  return "unknown";
}

}  // namespace iceberg::filter
