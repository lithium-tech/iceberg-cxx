#include "iceberg/src/type.h"

#include <optional>
#include <string>
#include <string_view>

namespace iceberg {

struct StringToTypeEntry {
  std::string_view name;
  Type type;
};

constexpr StringToTypeEntry kStringToPrimitiveTypeId[] = {{"boolean", Type::kBoolean},
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

std::optional<std::string> TypeToName(Type type) {
  for (const auto& [entry_name, entry_type] : kStringToPrimitiveTypeId) {
    if (entry_type == type) {
      return std::string(entry_name);
    }
  }
  return std::nullopt;
}

};  // namespace iceberg
