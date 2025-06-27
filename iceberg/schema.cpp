#include "iceberg/schema.h"

#include <algorithm>

namespace {

inline bool EqualCharIgnoreCase(char a, char b) {
  return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
}

inline bool EqualIgnoreCase(const std::string& a, const std::string& b) {
  return std::equal(a.begin(), a.end(), b.begin(), b.end(), EqualCharIgnoreCase);
}

inline bool HasPhysicalType(const parquet::schema::Node* node, parquet::Type::type type) {
  return node->is_primitive() && static_cast<const parquet::schema::PrimitiveNode*>(node)->physical_type() == type;
}

inline int32_t GetTypeLength(const parquet::schema::Node* node) {
  return static_cast<const parquet::schema::PrimitiveNode*>(node)->type_length();
}

inline std::string PhysicalTypeErrorMessage(std::shared_ptr<const iceberg::types::Type> iceberg_type,
                                            parquet::Type::type parquet_type) {
  return "Iceberg " + iceberg_type->ToString() + " column must be represented as parquet " +
         parquet::TypeToString(parquet_type) + " physical type";
}

inline std::string LogicalTypeErrorMessage(std::shared_ptr<const iceberg::types::Type> iceberg_type,
                                           std::shared_ptr<const parquet::LogicalType> parquet_type) {
  return "Iceberg " + iceberg_type->ToString() + " column must be represented as parquet " + parquet_type->ToString() +
         " logical type";
}

std::string UniteErrorMessages(std::vector<std::string>&& error_messages) {
  std::string res;
  for (auto& message : error_messages) {
    res += std::move(message) + '\n';
  }
  return res;
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

bool IcebergToParquetSchemaValidator::Ensure(bool cond, const std::string& message,
                                             std::vector<std::string>& error_log) {
  if (!cond) {
    error_log.push_back(message);
  }
  return cond;
}

const std::array<std::pair<TypeID, parquet::Type::type>, 13> IcebergToParquetSchemaValidator::map_ = {
    std::pair{TypeID::kBoolean, parquet::Type::BOOLEAN},
    {TypeID::kInt, parquet::Type::INT32},
    {TypeID::kLong, parquet::Type::INT64},
    {TypeID::kFloat, parquet::Type::FLOAT},
    {TypeID::kDouble, parquet::Type::DOUBLE},
    {TypeID::kDate, parquet::Type::INT32},
    {TypeID::kTime, parquet::Type::INT64},
    {TypeID::kTimestamp, parquet::Type::INT64},
    {TypeID::kTimestamptz, parquet::Type::INT64},
    {TypeID::kTimestampNs, parquet::Type::INT64},
    {TypeID::kTimestamptzNs, parquet::Type::INT64},
    {TypeID::kString, parquet::Type::BYTE_ARRAY},
    {TypeID::kBinary, parquet::Type::BYTE_ARRAY},
};

void IcebergToParquetSchemaValidator::ValidateColumn(const types::NestedField& field, const parquet::schema::Node* node,
                                                     std::vector<std::string>& error_log) {
  switch (field.type->TypeId()) {
    case TypeID::kBoolean:
    case TypeID::kInt:
    case TypeID::kLong:
    case TypeID::kFloat:
    case TypeID::kDouble:
    case TypeID::kBinary: {
      break;
    }
    case TypeID::kDecimal: {
      auto precision = std::static_pointer_cast<const types::DecimalType>(field.type)->Precision();
      auto scale = std::static_pointer_cast<const types::DecimalType>(field.type)->Scale();
      if (Ensure(node->logical_type()->is_decimal(),
                 LogicalTypeErrorMessage(field.type, parquet::LogicalType::Decimal(precision, scale)), error_log)) {
        auto decimal_node = std::static_pointer_cast<const parquet::DecimalLogicalType>(node->logical_type());
        Ensure(decimal_node->precision() == precision && decimal_node->scale() == scale,
               LogicalTypeErrorMessage(field.type, parquet::LogicalType::Decimal(precision, scale)), error_log);
      }

      if (precision <= 9) {
        Ensure(HasPhysicalType(node, parquet::Type::INT32), PhysicalTypeErrorMessage(field.type, parquet::Type::INT32),
               error_log);
      } else if (precision <= 18) {
        Ensure(HasPhysicalType(node, parquet::Type::INT64), PhysicalTypeErrorMessage(field.type, parquet::Type::INT64),
               error_log);
      } else {
        if (Ensure(HasPhysicalType(node, parquet::Type::FIXED_LEN_BYTE_ARRAY),
                   PhysicalTypeErrorMessage(field.type, parquet::Type::FIXED_LEN_BYTE_ARRAY), error_log)) {
          Ensure(GetTypeLength(node) == arrow::DecimalType::DecimalSize(precision),
                 "Iceberg " + field.type->ToString() +
                     " column type length must use minimal number of bytes that can store precision",
                 error_log);
        }
      }
      break;
    }

    case TypeID::kDate: {
      Ensure(node->logical_type()->is_date(), LogicalTypeErrorMessage(field.type, parquet::LogicalType::Date()),
             error_log);
      break;
    }

    case TypeID::kTime: {
      if (Ensure(node->logical_type()->is_time(),
                 LogicalTypeErrorMessage(field.type,
                                         parquet::LogicalType::Time(false, parquet::LogicalType::TimeUnit::MICROS)),
                 error_log)) {
        auto time_node = std::static_pointer_cast<const parquet::TimeLogicalType>(node->logical_type());
        Ensure(!time_node->is_adjusted_to_utc() && time_node->time_unit() == parquet::LogicalType::TimeUnit::MICROS,
               LogicalTypeErrorMessage(field.type,
                                       parquet::LogicalType::Time(false, parquet::LogicalType::TimeUnit::MICROS)),
               error_log);
      }
      break;
    }

    case TypeID::kTimestamp: {
      if (Ensure(node->logical_type()->is_timestamp(),
                 LogicalTypeErrorMessage(
                     field.type, parquet::LogicalType::Timestamp(false, parquet::LogicalType::TimeUnit::MICROS)),
                 error_log)) {
        auto timestamp_node = std::static_pointer_cast<const parquet::TimestampLogicalType>(node->logical_type());
        Ensure(!timestamp_node->is_adjusted_to_utc() &&
                   timestamp_node->time_unit() == parquet::LogicalType::TimeUnit::MICROS,
               LogicalTypeErrorMessage(field.type,
                                       parquet::LogicalType::Timestamp(false, parquet::LogicalType::TimeUnit::MICROS)),
               error_log);
      }
      break;
    }

    case TypeID::kTimestamptz: {
      if (Ensure(node->logical_type()->is_timestamp(),
                 LogicalTypeErrorMessage(field.type,
                                         parquet::LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::MICROS)),
                 error_log)) {
        auto timestamp_node = std::static_pointer_cast<const parquet::TimestampLogicalType>(node->logical_type());
        Ensure(timestamp_node->is_adjusted_to_utc() &&
                   timestamp_node->time_unit() == parquet::LogicalType::TimeUnit::MICROS,
               LogicalTypeErrorMessage(field.type,
                                       parquet::LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::MICROS)),
               error_log);
      }
      break;
    }

    case TypeID::kTimestampNs: {
      if (Ensure(node->logical_type()->is_timestamp(),
                 LogicalTypeErrorMessage(field.type,
                                         parquet::LogicalType::Timestamp(false, parquet::LogicalType::TimeUnit::NANOS)),
                 error_log)) {
        auto timestamp_node = std::static_pointer_cast<const parquet::TimestampLogicalType>(node->logical_type());
        Ensure(!timestamp_node->is_adjusted_to_utc() &&
                   timestamp_node->time_unit() == parquet::LogicalType::TimeUnit::NANOS,
               LogicalTypeErrorMessage(field.type,
                                       parquet::LogicalType::Timestamp(false, parquet::LogicalType::TimeUnit::NANOS)),
               error_log);
      }
      break;
    }

    case TypeID::kTimestamptzNs: {
      if (Ensure(node->logical_type()->is_timestamp(),
                 LogicalTypeErrorMessage(field.type,
                                         parquet::LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::NANOS)),
                 error_log)) {
        auto timestamp_node = std::static_pointer_cast<const parquet::TimestampLogicalType>(node->logical_type());
        Ensure(timestamp_node->is_adjusted_to_utc() &&
                   timestamp_node->time_unit() == parquet::LogicalType::TimeUnit::NANOS,
               LogicalTypeErrorMessage(field.type,
                                       parquet::LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::NANOS)),
               error_log);
      }
      break;
    }

    case TypeID::kString: {
      Ensure(node->logical_type()->is_string(), LogicalTypeErrorMessage(field.type, parquet::LogicalType::String()),
             error_log);
      Ensure(node->converted_type() == parquet::ConvertedType::UTF8,
             "Iceberg " + field.type->ToString() + " column must be encoded in UTF-8", error_log);
      break;
    }

    case TypeID::kUuid: {
      Ensure(node->logical_type()->is_UUID(), LogicalTypeErrorMessage(field.type, parquet::LogicalType::UUID()),
             error_log);
      Ensure(HasPhysicalType(node, parquet::Type::FIXED_LEN_BYTE_ARRAY) && GetTypeLength(node) == 16,
             PhysicalTypeErrorMessage(field.type, parquet::Type::FIXED_LEN_BYTE_ARRAY) + " with type length = 16",
             error_log);
      ;
      break;
    }

    case TypeID::kFixed: {
      Ensure(HasPhysicalType(node, parquet::Type::FIXED_LEN_BYTE_ARRAY) &&
                 GetTypeLength(node) == std::static_pointer_cast<const types::FixedType>(field.type)->Size(),
             PhysicalTypeErrorMessage(field.type, parquet::Type::FIXED_LEN_BYTE_ARRAY) + " with type length = " +
                 std::to_string(std::static_pointer_cast<const types::FixedType>(field.type)->Size()),
             error_log);
      break;
    }
    case TypeID::kList: {
      const std::string error_message =
          "Iceberg " + field.type->ToString() + " column must be represented as parquet 3-level list";
      Ensure(node->logical_type()->is_list(), LogicalTypeErrorMessage(field.type, parquet::LogicalType::List()),
             error_log);
      if (Ensure(node->is_group(), error_message, error_log)) {
        auto group_node = static_cast<const parquet::schema::GroupNode*>(node);
        if (Ensure(group_node->field_count() == 1 && group_node->field(0)->is_group(), error_message, error_log)) {
          auto child_node = std::static_pointer_cast<const parquet::schema::GroupNode>(group_node->field(0));
          Ensure(child_node->field_count() == 1, error_message, error_log);
        }
      }

      break;
    }

    default: {
      Ensure(false, "Unknown/Unsupported Iceberg type", error_log);
      break;
    }
  }
  for (const auto& [iceberg_type, parquet_type] : map_) {
    if (field.type->TypeId() == iceberg_type) {
      Ensure(HasPhysicalType(node, parquet_type), PhysicalTypeErrorMessage(field.type, parquet_type), error_log);
    }
  }
}

bool IcebergToParquetSchemaValidator::Validate(const Schema& iceberg_schema,
                                               const parquet::SchemaDescriptor& parquet_schema, bool throws_on_error) {
  const auto& ice_cols = iceberg_schema.Columns();
  std::vector<std::string> error_log;
  std::map<int, int> fieldid_to_index;
  for (int i = 0; i < parquet_schema.num_columns(); ++i) {
    auto field_id = parquet_schema.GetColumnRoot(i)->field_id();
    if (field_id >= 0) {
      fieldid_to_index[field_id] = i;
    }
  }
  for (const auto& ice_col : ice_cols) {
    if (fieldid_to_index.contains(ice_col.field_id)) {
      ValidateColumn(ice_col, parquet_schema.GetColumnRoot(fieldid_to_index.at(ice_col.field_id)), error_log);
    }
  }
  if (!error_log.empty()) {
    if (throws_on_error) {
      throw std::runtime_error(UniteErrorMessages(std::move(error_log)));
    }
    return false;
  }
  return true;
}

}  // namespace iceberg
