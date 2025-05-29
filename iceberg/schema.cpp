#include "iceberg/schema.h"

#include <algorithm>

namespace {

inline bool EqualCharIgnoreCase(char a, char b) {
  return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
}

inline bool EqualIgnoreCase(const std::string& a, const std::string& b) {
  return std::equal(a.begin(), a.end(), b.begin(), b.end(), EqualCharIgnoreCase);
}

}  // namespace

namespace iceberg {

std::optional<int32_t> Schema::FindMatchingColumn(
    const std::function<bool(const types::NestedField&)> predicate) const {
  for (const auto& field : fields_) {
    if (predicate(field)) {
      return field.field_id;
    }
  }
  return std::nullopt;
}

std::optional<int32_t> Schema::FindColumn(const std::string& column_name) const {
  return FindMatchingColumn([&column_name](const types::NestedField& field) { return field.name == column_name; });
}

std::optional<int32_t> Schema::FindColumnIgnoreCase(const std::string& column_name) const {
  return FindMatchingColumn(
      [&column_name](const types::NestedField& field) { return EqualIgnoreCase(field.name, column_name); });
}

void Schema::FilterColumns(const std::unordered_set<int>& ids_to_remove) {
  if (ids_to_remove.empty()) {
    return;
  }
  auto it = std::remove_if(fields_.begin(), fields_.end(), [&ids_to_remove](const types::NestedField& field) {
    return ids_to_remove.contains(field.field_id);
  });
  fields_.erase(it, fields_.end());
}

void IcebergToParquetSchemaValidator::Ensure(bool cond, const std::string& message, std::string& error_log) {
  if (!cond) {
    error_log += message;
  }
}

void IcebergToParquetSchemaValidator::ValidateColumn(const types::NestedField& field, const parquet::schema::Node* node,
                                                     std::string& error_log) {
  Ensure(field.field_id == node->field_id(), "Iceberg field_id must match parquet field_id\n", error_log);
  switch (field.type->TypeId()) {
    case TypeID::kBoolean: {
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::BOOLEAN,
             "Iceberg Boolean column must be represented as parquet BOOLEAN physical type\n", error_log);
      break;
    }

    case TypeID::kInt: {
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::INT32,
             "Iceberg Int column must be represented as parquet INT32 physical type\n", error_log);
      break;
    }

    case TypeID::kLong: {
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::INT64,
             "Iceberg Long column must be represented as parquet INT64 physical type\n", error_log);
      break;
    }

    case TypeID::kFloat: {
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::FLOAT,
             "Iceberg Float column must be represented as parquet FLOAT physical type\n", error_log);
      break;
    }

    case TypeID::kDouble: {
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::DOUBLE,
             "Iceberg Double column must be represented as parquet DOUBLE physical type\n", error_log);
      break;
    }

    case TypeID::kDecimal: {
      auto precision = std::static_pointer_cast<const types::DecimalType>(field.type)->Precision();
      auto scale = std::static_pointer_cast<const types::DecimalType>(field.type)->Scale();
      Ensure(node->logical_type()->is_decimal(),
             "Iceberg Decimal(P, S) column must be represented as parquet Decimal(P, S) logical type\n", error_log);
      auto decimal_node = std::static_pointer_cast<const parquet::DecimalLogicalType>(node->logical_type());
      Ensure(decimal_node->precision() == precision && decimal_node->scale() == scale,
             "Iceberg Decimal(P, S) column must be represented as parquet Decimal(P, S) logical type\n", error_log);
      if (precision <= 9) {
        Ensure(node->is_primitive() &&
                   static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::INT32,
               "Iceberg Decimal(P, S) column with P <= 9 must be represented as parquet INT32 physical type\n",
               error_log);
      } else if (precision <= 18) {
        Ensure(node->is_primitive() &&
                   static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::INT64,
               "Iceberg Decimal(P, S) column with P <= 18 must be represented as parquet INT64 physical type\n",
               error_log);
      } else {
        Ensure(node->is_primitive() && static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() ==
                                           parquet::Type::FIXED_LEN_BYTE_ARRAY,
               "Iceberg Decimal(P, S) column with P > 18 must be represented as parquet FIXED_LEN_BYTE_ARRAY physical "
               "type\n",
               error_log);
        Ensure(
            node->is_primitive() && static_cast<const parquet::schema::PrimitiveNode*>(node)->type_length() ==
                                        static_cast<int32_t>(std::ceil((precision * log2(10) + 1) / CHAR_BIT)),
            "Iceberg Decimal(P, S) column with P > 18 type length must use minimal number of bytes that can store P\n",
            error_log);
        // precision * log2(10) bits for digits, 1 for sign, divided by CHAR_BIT and rounded up
      }
      break;
    }

    case TypeID::kDate: {
      Ensure(node->logical_type()->is_date(), "Iceberg Date column must be represented as parquet DATE logical type\n",
             error_log);
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::INT32,
             "Iceberg Date column must be represented as parquet INT32 physical type\n", error_log);
      break;
    }

    case TypeID::kTime: {
      Ensure(node->logical_type()->is_time(),
             "Iceberg Time column must be represented as parquet Time logical type with adjustToUtc=false and "
             "TimeUnit::MICROS\n",
             error_log);
      auto time_node = std::static_pointer_cast<const parquet::TimeLogicalType>(node->logical_type());
      Ensure(!time_node->is_adjusted_to_utc() && time_node->time_unit() == parquet::LogicalType::TimeUnit::MICROS,
             "Iceberg Time column must be represented as parquet Time logical type with adjustToUtc=false and "
             "TimeUnit::MICROS\n",
             error_log);
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::INT64,
             "Iceberg Time column must be represented as parquet INT64 physical type\n", error_log);
      break;
    }

    case TypeID::kTimestamp: {
      Ensure(
          node->logical_type()->is_timestamp(),
          "Iceberg Timestamp column must be represented as parquet Timestamp logical type with adjustToUtc=false and "
          "TimeUnit::MICROS\n",
          error_log);
      auto timestamp_node = std::static_pointer_cast<const parquet::TimestampLogicalType>(node->logical_type());
      Ensure(
          !timestamp_node->is_adjusted_to_utc() &&
              timestamp_node->time_unit() == parquet::LogicalType::TimeUnit::MICROS,
          "Iceberg Timestamp column must be represented as parquet Timestamp logical type with adjustToUtc=false and "
          "TimeUnit::MICROS\n",
          error_log);
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::INT64,
             "Iceberg Timestamp column must be represented as parquet INT64 physical type\n", error_log);
      break;
    }

    case TypeID::kTimestamptz: {
      Ensure(
          node->logical_type()->is_timestamp(),
          "Iceberg Timestamptz column must be represented as parquet Timestamp logical type with adjustToUtc=true and "
          "TimeUnit::MICROS\n",
          error_log);
      auto timestamp_node = std::static_pointer_cast<const parquet::TimestampLogicalType>(node->logical_type());
      Ensure(
          timestamp_node->is_adjusted_to_utc() && timestamp_node->time_unit() == parquet::LogicalType::TimeUnit::MICROS,
          "Iceberg Timestamptz column must be represented as parquet Timestamp logical type with adjustToUtc=true and "
          "TimeUnit::MICROS\n",
          error_log);
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::INT64,
             "Iceberg Timestamptz column must be represented as parquet INT64 physical type\n", error_log);
      break;
    }

    case TypeID::kString: {
      Ensure(node->logical_type()->is_string(), "Iceberg String column must be represented as String logical type\n",
             error_log);
      Ensure(node->converted_type() == parquet::ConvertedType::UTF8, "Iceberg String column must be encoded in UTF-8\n",
             error_log);
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::BYTE_ARRAY,
             "Iceberg String column must be represented as parquet BYTE_ARRAY physical type\n", error_log);
      break;
    }

    case TypeID::kUuid: {
      Ensure(node->logical_type()->is_UUID(), "Iceberg Uuid column must be represented as UUID logical type\n",
             error_log);
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() ==
                     parquet::Type::FIXED_LEN_BYTE_ARRAY &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->type_length() == 16,
             "Iceberg Uuid must be represented as parquet FIXED_LEN_BYTE_ARRAY[16] physical type\n", error_log);
      break;
    }

    case TypeID::kFixed: {
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() ==
                     parquet::Type::FIXED_LEN_BYTE_ARRAY &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->type_length() ==
                     std::static_pointer_cast<const types::FixedType>(field.type)->Size(),
             "Iceberg Fixed(L) must be represented as parquet FIXED_LEN_BYTE_ARRAY[L] physical type\n", error_log);
      break;
    }
    case TypeID::kBinary: {
      Ensure(node->is_primitive() &&
                 static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == parquet::Type::BYTE_ARRAY,
             "Iceberg BINARY column must be represented as parquet BYTE_ARRAY physical type\n", error_log);
      break;
    }

    case TypeID::kList: {
      Ensure(node->logical_type()->is_list(), "Iceberg List column must be represented as List logical type\n",
             error_log);
      Ensure(node->is_group(), "Iceberg List column must be represented as parquet 3-level list\n", error_log);
      auto group_node = static_cast<const parquet::schema::GroupNode*>(node);
      Ensure(group_node->field_count() == 1 && group_node->field(0)->is_group(),
             "Iceberg List column must be represented as parquet 3-level list\n", error_log);
      auto child_node = std::static_pointer_cast<const parquet::schema::GroupNode>(group_node->field(0));
      Ensure(child_node->field_count() == 1, "Iceberg List column must be represented as parquet 3-level list\n",
             error_log);
      break;
    }

    default: {
      Ensure(false, "Unknown/Unsupported Iceberg type\n", error_log);
      break;
    }
  }
}

bool IcebergToParquetSchemaValidator::Validate(const Schema& iceberg_schema,
                                               const parquet::SchemaDescriptor& parquet_schema, bool throws_on_error) {
  const auto& ice_cols = iceberg_schema.Columns();
  std::string error_log;
  Ensure(ice_cols.size() == parquet_schema.num_columns(),
         "iceberg and parquet schemas have different number of columns\n", error_log);
  if (!error_log.empty()) {
    if (throws_on_error) {
      throw std::runtime_error(error_log);
    }
    return false;
  }
  for (int i = 0; i < ice_cols.size(); ++i) {
    ValidateColumn(ice_cols[i], parquet_schema.GetColumnRoot(i), error_log);
  }
  if (!error_log.empty()) {
    if (throws_on_error) {
      throw std::runtime_error(error_log);
    }
    return false;
  }
  return true;
}

}  // namespace iceberg
