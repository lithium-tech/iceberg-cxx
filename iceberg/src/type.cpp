#include "iceberg/src/type.h"

#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>

namespace iceberg {

namespace {

struct StringToTypeEntry {
  std::string_view name;
  Type type;
};

constexpr StringToTypeEntry kStringToPrimitiveTypeId[] = {
    {"boolean", Type::kBoolean},
    {"int", Type::kInt},
    {"long", Type::kLong},
    {"float", Type::kFloat},
    {"double", Type::kDouble},
    {"date", Type::kDate},
    {"time", Type::kTime},
    {"timestamp", Type::kTimestamp},
    {"timestamptz", Type::kTimestamptz},
    {"string", Type::kString},
    {"uuid", Type::kUuid},
    {"binary", Type::kBinary}};

std::optional<Type> NameToType(const std::string& name_to_find) {
  for (const auto& [name, type] : kStringToPrimitiveTypeId) {
    if (name == name_to_find) {
      return type;
    }
  }
  return std::nullopt;
}
}  // namespace

std::string PrimitiveDataType::ToString() const {
  for (const auto& [name, type] : kStringToPrimitiveTypeId) {
    if (type == id_) {
      return std::string(name);
    }
  }
  throw std::runtime_error(
      "Internal error in tea. PrimitiveDataType::ToString()");
}

std::string DecimalDataType::ToString() const {
  return "decimal(" + std::to_string(precision_) + ", " +
         std::to_string(scale_) + ")";
}

std::shared_ptr<const DataType> StringToDataType(const std::string& str) {
  if (auto maybe_value = NameToType(str); maybe_value.has_value()) {
    return std::make_shared<PrimitiveDataType>(maybe_value.value());
  }
  if (str.starts_with("decimal")) {
    // decimal(P, S)
    std::stringstream ss(str);
    ss.ignore(std::string("decimal(").size());
    int32_t precision = -1;
    int32_t scale = -1;
    ss >> precision;
    ss.ignore(1);  // skip comma
    ss >> scale;
    return std::make_shared<DecimalDataType>(precision, scale);
  }
  throw std::runtime_error("Type " + str + " is not supported");

  // TODO(gmusya): support binary, struct, list, map
}

}  // namespace iceberg
