#include "iceberg/src/type.h"

#include <optional>
#include <sstream>

namespace iceberg {

namespace {

struct StringToTypeEntry {
  std::string_view name;
  Type type;
};

constexpr StringToTypeEntry kStringToPrimitiveTypeId[] = {
    {"boolean", Type::BOOLEAN},
    {"int", Type::INT},
    {"long", Type::LONG},
    {"float", Type::FLOAT},
    {"double", Type::DOUBLE},
    {"date", Type::DATE},
    {"time", Type::TIME},
    {"timestamp", Type::TIMESTAMP},
    {"timestamptz", Type::TIMESTAMPTZ},
    {"string", Type::STRING},
    {"uuid", Type::UUID},
    {"binary", Type::BINARY}};

std::optional<Type> NameToType(const std::string& name_to_find) {
  for (const auto& [name, type] : kStringToPrimitiveTypeId) {
    if (name == name_to_find) {
      return type;
    }
  }
  return std::nullopt;
}
}  // namespace

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
