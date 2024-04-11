#include "iceberg/src/type.h"

#include <optional>
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

};  // namespace iceberg::types
