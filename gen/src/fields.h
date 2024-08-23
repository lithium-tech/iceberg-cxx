#pragma once

#include <memory>
#include <stdexcept>
#include <string>

#include "arrow/type.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace gen {

static constexpr int kUnsetLength = -1;
static constexpr int kUnsetPrecision = -1;
static constexpr int kUnsetScale = -1;

inline std::shared_ptr<parquet::schema::Node> MakePrimitiveInt32Node(std::string_view name, int32_t field_id = -1) {
  return parquet::schema::PrimitiveNode::Make(std::string(name), parquet::Repetition::REQUIRED, parquet::Type::INT32,
                                              parquet::ConvertedType::INT_32, kUnsetLength, kUnsetPrecision,
                                              kUnsetScale, field_id);
}

inline std::shared_ptr<parquet::schema::Node> MakePrimitiveInt64Node(std::string_view name, int32_t field_id = -1) {
  return parquet::schema::PrimitiveNode::Make(std::string(name), parquet::Repetition::REQUIRED, parquet::Type::INT64,
                                              parquet::ConvertedType::INT_64, kUnsetLength, kUnsetPrecision,
                                              kUnsetScale, field_id);
}

inline std::shared_ptr<parquet::schema::Node> MakeStringNode(std::string_view name, int32_t field_id = -1) {
  return parquet::schema::PrimitiveNode::Make(std::string(name), parquet::Repetition::REQUIRED,
                                              parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8, kUnsetLength,
                                              kUnsetPrecision, kUnsetScale, field_id);
}

inline parquet::Type::type GetDecimalPhysicalType(int32_t precision) {
  if (precision <= 0) {
    throw std::runtime_error("Precision must be positive");
  } else if (precision <= 9) {
    return parquet::Type::INT32;
  } else if (precision <= 18) {
    return parquet::Type::INT64;
  } else if (precision <= 38) {
    return parquet::Type::FIXED_LEN_BYTE_ARRAY;
  }
  throw std::runtime_error("Precision must be less than 39");
}

// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
// Length n can store <= floor(log_10(2^(8*n - 1) - 1)) base-10 digits
inline int32_t GetDecimalFLBALength(int32_t precision) { return 16; }

inline std::shared_ptr<parquet::schema::Node> MakeDecimalNode(std::string_view name, int32_t precision, int32_t scale,
                                                              int32_t field_id = -1) {
  auto physical_type = GetDecimalPhysicalType(precision);
  if (physical_type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    auto length = GetDecimalFLBALength(precision);
    return parquet::schema::PrimitiveNode::Make(std::string(name), parquet::Repetition::REQUIRED, physical_type,
                                                parquet::ConvertedType::DECIMAL, length, precision, scale, field_id);
  }
  return parquet::schema::PrimitiveNode::Make(std::string(name), parquet::Repetition::REQUIRED, physical_type,
                                              parquet::ConvertedType::DECIMAL, -1, precision, scale, field_id);
}

inline std::shared_ptr<parquet::schema::Node> MakeDateNode(std::string_view name, int32_t field_id = -1) {
  return parquet::schema::PrimitiveNode::Make(std::string(name), parquet::Repetition::REQUIRED, parquet::Type::INT32,
                                              parquet::ConvertedType::DATE, -1, -1, -1, field_id);
}

inline std::shared_ptr<parquet::schema::Node> ParquetNodeFromArrowField(const std::shared_ptr<arrow::Field>& field) {
  switch (field->type()->id()) {
    case arrow::Type::STRING:
      return MakeStringNode(field->name());
    case arrow::Type::INT32:
      return MakePrimitiveInt32Node(field->name());
    case arrow::Type::INT64:
      return MakePrimitiveInt64Node(field->name());
    case arrow::Type::DATE32:
      return MakeDateNode(field->name());
    case arrow::Type::DECIMAL: {
      auto dec_field = std::static_pointer_cast<arrow::Decimal128Type>(field->type());
      return MakeDecimalNode(field->name(), dec_field->precision(), dec_field->scale());
    }
    default:
      throw std::runtime_error("Unexpected field: " + field->ToString());
  }
  return nullptr;
}

inline std::shared_ptr<parquet::schema::GroupNode> ParquetSchemaFromArrowSchema(
    std::shared_ptr<const arrow::Schema> arrow_schema) {
  parquet::schema::NodeVector node_vector;
  for (const auto& field : arrow_schema->fields()) {
    node_vector.emplace_back(ParquetNodeFromArrowField(field));
  }
  return std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, node_vector));
}

}  // namespace gen
