#include "iceberg/type.h"

#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace iceberg::types {

struct StringToTypeEntry {
  std::string_view name;
  TypeID type;
};

constexpr StringToTypeEntry kStringToPrimitiveTypeId[] = {{"boolean", TypeID::kBoolean},
                                                          {"int", TypeID::kInt},
                                                          {"long", TypeID::kLong},
                                                          {"float", TypeID::kFloat},
                                                          {"double", TypeID::kDouble},
                                                          {"date", TypeID::kDate},
                                                          {"time", TypeID::kTime},
                                                          {"timestamp", TypeID::kTimestamp},
                                                          {"timestamptz", TypeID::kTimestamptz},
                                                          {"string", TypeID::kString},
                                                          {"uuid", TypeID::kUuid},
                                                          {"binary", TypeID::kBinary}};

std::optional<TypeID> NameToPrimitiveType(const std::string& name_to_find) {
  for (const auto& [name, type] : kStringToPrimitiveTypeId) {
    if (name == name_to_find) {
      return type;
    }
  }
  return std::nullopt;
}

std::optional<std::string> PrimitiveTypeToName(TypeID type) {
  for (const auto& [entry_name, entry_type] : kStringToPrimitiveTypeId) {
    if (entry_type == type) {
      return std::string(entry_name);
    }
  }
  return std::nullopt;
}

std::shared_ptr<Type> ConvertArrowTypeToIceberg(const std::shared_ptr<arrow::DataType>& type, int field_id) {
  switch (type->id()) {
    case arrow::Type::BOOL: {
      return std::make_shared<PrimitiveType>(TypeID::kBoolean);
    }
    case arrow::Type::INT8: {
      return std::make_shared<PrimitiveType>(TypeID::kInt);
    }
    case arrow::Type::INT16: {
      return std::make_shared<PrimitiveType>(TypeID::kInt);
    }
    case arrow::Type::INT32: {
      return std::make_shared<PrimitiveType>(TypeID::kInt);
    }
    case arrow::Type::INT64: {
      return std::make_shared<PrimitiveType>(TypeID::kLong);
    }
    case arrow::Type::FLOAT: {
      return std::make_shared<PrimitiveType>(TypeID::kFloat);
    }
    case arrow::Type::DOUBLE: {
      return std::make_shared<PrimitiveType>(TypeID::kDouble);
    }
    case arrow::Type::DECIMAL: {
      auto decimal_type = std::static_pointer_cast<arrow::DecimalType>(type);
      return std::make_shared<DecimalType>(decimal_type->precision(), decimal_type->scale());
    }
    case arrow::Type::DATE32: {
      throw std::runtime_error("arrow::Type::DATE32 is not supported in iceberg-cpp");
    }
    case arrow::Type::DATE64: {
      return std::make_shared<PrimitiveType>(TypeID::kDate);
    }
    case arrow::Type::TIME32:
      throw std::runtime_error("arrow::Type::TIME32 is not supported in iceberg-cpp");
    case arrow::Type::TIME64:
      return std::make_shared<PrimitiveType>(TypeID::kTime);
    case arrow::Type::TIMESTAMP: {
      auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(type);
      return std::make_shared<PrimitiveType>(timestamp_type->timezone().empty() ? TypeID::kTimestamp
                                                                                : TypeID::kTimestamptz);
    }
    case arrow::Type::STRING: {
      return std::make_shared<PrimitiveType>(TypeID::kString);
    }
    case arrow::Type::LIST: {
      auto list_type = std::static_pointer_cast<arrow::ListType>(type);
      return std::make_shared<ListType>(field_id, true, ConvertArrowTypeToIceberg(list_type->value_type(), field_id));
    }
    default:
      return std::make_shared<PrimitiveType>(TypeID::kUnknown);
  }
}
};  // namespace iceberg::types
