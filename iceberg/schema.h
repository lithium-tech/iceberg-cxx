#pragma once

#include <algorithm>
#include <cctype>
#include <functional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "iceberg/nested_field.h"

namespace iceberg {

class Schema {
 public:
  Schema(int32_t schema_id, const std::vector<types::NestedField>& fields) : schema_id_(schema_id), fields_(fields) {}

  Schema(int32_t schema_id, std::vector<types::NestedField>&& fields)
      : schema_id_(schema_id), fields_(std::move(fields)) {}

  int32_t SchemaId() const { return schema_id_; }
  const std::vector<types::NestedField>& Columns() const { return fields_; }

  std::optional<int32_t> FindMatchingColumn(const std::function<bool(const types::NestedField&)> predicate) const;
  std::optional<int32_t> FindColumn(const std::string& column_name) const;
  std::optional<int32_t> FindColumnIgnoreCase(const std::string& column_name) const;
  void FilterColumns(const std::unordered_set<int>& ids_to_remove);

  template <typename T>
  std::unordered_set<int> FindColumnIds(const T& filter) {
    if (filter.empty()) {
      return {};
    }
    std::unordered_set<int> ids;
    for (auto& col_name : filter) {
      if (auto opt = FindColumnIgnoreCase(col_name); opt.has_value()) {
        ids.insert(*opt);
      }
    }
    return ids;
  }

 private:
  int32_t schema_id_;
  std::vector<types::NestedField> fields_;
};

}  // namespace iceberg
