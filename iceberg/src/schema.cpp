#include "iceberg/src/schema.h"

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

}  // namespace iceberg
