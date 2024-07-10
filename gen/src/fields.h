#pragma once

#include <memory>
#include <string>

#include "arrow/type.h"
#include "parquet/schema.h"

namespace gen {

inline std::shared_ptr<parquet::schema::Node> MakePrimitiveInt32Node(std::string_view name, int32_t field_id = -1) {
  return parquet::schema::PrimitiveNode::Make(std::string(name), parquet::Repetition::REQUIRED, parquet::Type::INT32,
                                              parquet::ConvertedType::NONE, -1, -1, -1, field_id);
}

inline std::shared_ptr<parquet::schema::Node> MakeStringNode(std::string_view name, int32_t field_id = -1) {
  return parquet::schema::PrimitiveNode::Make(std::string(name), parquet::Repetition::REQUIRED,
                                              parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8, -1, -1, -1,
                                              field_id);
}

inline std::shared_ptr<parquet::schema::Node> MakeDecimalNode(std::string_view name, int32_t precision, int32_t scale,
                                                              int32_t field_id = -1) {
  return parquet::schema::PrimitiveNode::Make(std::string(name), parquet::Repetition::REQUIRED, parquet::Type::INT32,
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
    case arrow::Type::DATE32:
      return MakeDateNode(field->name());
    case arrow::Type::DECIMAL: {
      auto dec_field = std::static_pointer_cast<arrow::Decimal128Type>(field->type());
      return MakeDecimalNode(dec_field->name(), dec_field->precision(), dec_field->scale());
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
