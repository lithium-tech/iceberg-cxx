#include "iceberg/test_utils/column.h"

#include <utility>
#include <variant>

#include "parquet/schema.h"
#include "parquet/types.h"

namespace iceberg {

namespace {
OptionalVector<parquet::ByteArray> ToByteArrayVector(const std::vector<std::string*>& data) {
  OptionalVector<parquet::ByteArray> result;
  for (const std::string* ptr : data) {
    if (!ptr) {
      result.emplace_back(std::nullopt);
    } else {
      parquet::ByteArray byte_array(ptr->size(), reinterpret_cast<const uint8_t*>(ptr->c_str()));
      result.emplace_back(std::move(byte_array));
    }
  }
  return result;
}

OptionalVector<parquet::FixedLenByteArray> ToFLBAVector(const std::vector<std::string*>& data) {
  OptionalVector<parquet::FixedLenByteArray> result;
  for (const std::string* ptr : data) {
    if (!ptr) {
      result.emplace_back(std::nullopt);
    } else {
      parquet::FixedLenByteArray flba(reinterpret_cast<const uint8_t*>(ptr->c_str()));
      result.emplace_back(std::move(flba));
    }
  }
  return result;
}

}  // namespace

parquet::schema::NodePtr ParquetInfo::MakeField() const {
  if (repetition == parquet::Repetition::REPEATED) {
    // Treat repeated fields as three-level list representation.
    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
    //
    // optional group <name> (LIST) {
    //   repeated group list {
    //     optional <element-type> element;
    //   }
    // }
    parquet::schema::NodePtr element = parquet::schema::PrimitiveNode::Make("element", parquet::Repetition::OPTIONAL,
                                                                            logical_type, physical_type, length);
    parquet::schema::NodePtr list = parquet::schema::GroupNode::Make("list", parquet::Repetition::REPEATED, {element});
    return parquet::schema::GroupNode::Make(name, parquet::Repetition::OPTIONAL, {list},
                                            parquet::ListLogicalType::Make(), field_id);
  } else {
    return parquet::schema::PrimitiveNode::Make(name, repetition, logical_type, physical_type, length, field_id);
  }
}

ParquetColumn MakeBoolColumn(const std::string& name, int field_id, const OptionalVector<bool>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::BOOLEAN,
                   .logical_type = parquet::LogicalType::None(),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeInt16Column(const std::string& name, int field_id, const OptionalVector<int32_t>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::INT32,
                   .logical_type = parquet::IntLogicalType::Make(16, true),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeInt32Column(const std::string& name, int field_id, const OptionalVector<int32_t>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::INT32,
                   .logical_type = parquet::IntLogicalType::Make(32, true),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeInt32ArrayColumn(const std::string& name, int field_id, const ArrayContainer& arrays) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::INT32,
                   .logical_type = parquet::IntLogicalType::Make(32, true),
                   .field_id = field_id,
                   .repetition = parquet::Repetition::REPEATED};
  return {info, arrays};
}

ParquetColumn MakeInt64Column(const std::string& name, int field_id, const OptionalVector<int64_t>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::INT64,
                   .logical_type = parquet::IntLogicalType::Make(64, true),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeFloatColumn(const std::string& name, int field_id, const OptionalVector<float>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::FLOAT,
                   .logical_type = parquet::LogicalType::None(),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeDoubleColumn(const std::string& name, int field_id, const OptionalVector<double>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::DOUBLE,
                   .logical_type = parquet::LogicalType::None(),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeJsonColumn(const std::string& name, int field_id, const std::vector<std::string*>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::BYTE_ARRAY,
                   .logical_type = parquet::JSONLogicalType::Make(),
                   .field_id = field_id};
  return {info, ToByteArrayVector(data)};
}

ParquetColumn MakeUuidColumn(const std::string& name, int field_id, const std::vector<std::string*>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::FIXED_LEN_BYTE_ARRAY,
                   .logical_type = parquet::UUIDLogicalType::Make(),
                   .field_id = field_id,
                   .length = 16};
  return {info, ToFLBAVector(data)};
}

ParquetColumn MakeBinaryColumn(const std::string& name, int field_id, const std::vector<std::string*>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::BYTE_ARRAY,
                   .logical_type = parquet::LogicalType::None(),
                   .field_id = field_id};
  return {info, ToByteArrayVector(data)};
}

ParquetColumn MakeStringColumn(const std::string& name, int field_id, const std::vector<std::string*>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::BYTE_ARRAY,
                   .logical_type = parquet::StringLogicalType::Make(),
                   .field_id = field_id};
  return {info, ToByteArrayVector(data)};
}

ParquetColumn MakeTimeColumn(const std::string& name, int field_id, const OptionalVector<int64_t>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::INT64,
                   .logical_type = parquet::TimeLogicalType::Make(false, parquet::LogicalType::TimeUnit::MICROS),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeTimestampColumn(const std::string& name, int field_id, const OptionalVector<int64_t>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::INT64,
                   .logical_type = parquet::TimestampLogicalType::Make(false, parquet::LogicalType::TimeUnit::MICROS),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeTimestamptzColumn(const std::string& name, int field_id, const OptionalVector<int64_t>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::INT64,
                   .logical_type = parquet::TimestampLogicalType::Make(true, parquet::LogicalType::TimeUnit::MICROS),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeDateColumn(const std::string& name, int field_id, const OptionalVector<int32_t>& data) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::INT32,
                   .logical_type = parquet::DateLogicalType::Make(),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeNumericColumn(const std::string& name, int field_id, const OptionalVector<int32_t>& data,
                                int precision, int scale) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::INT32,
                   .logical_type = parquet::DecimalLogicalType::Make(precision, scale),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeNumericColumn(const std::string& name, int field_id, const OptionalVector<int64_t>& data,
                                int precision, int scale) {
  ParquetInfo info{.name = name,
                   .physical_type = parquet::Type::INT64,
                   .logical_type = parquet::DecimalLogicalType::Make(precision, scale),
                   .field_id = field_id};
  return {info, data};
}

ParquetColumn MakeNumericColumn(const std::string& name, int field_id, const std::vector<std::string*>& data,
                                int precision, int scale, int length) {
  bool is_fixed_length = length != -1;
  ParquetInfo info{.name = name,
                   .physical_type = is_fixed_length ? parquet::Type::FIXED_LEN_BYTE_ARRAY : parquet::Type::BYTE_ARRAY,
                   .logical_type = parquet::DecimalLogicalType::Make(precision, scale),
                   .field_id = field_id,
                   .length = length};
  if (!is_fixed_length) {
    return {info, ToByteArrayVector(data)};
  } else {
    return {info, ToFLBAVector(data)};
  }
}

uint32_t GetSize(const ParquetColumnData& data) {
  return std::visit([](auto&& arg) -> uint32_t { return arg.size(); }, data);
}

}  // namespace iceberg
