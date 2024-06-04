#pragma once

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/src/nested_field.h"

namespace iceberg {

class Schema {
 public:
  Schema(int32_t schema_id, const std::vector<types::NestedField>& fields) : schema_id_(schema_id), fields_(fields) {}

  Schema(int32_t schema_id, std::vector<types::NestedField>&& fields)
      : schema_id_(schema_id), fields_(std::move(fields)) {}

  int32_t SchemaId() const { return schema_id_; }
  const std::vector<types::NestedField>& Columns() const { return fields_; }

  std::optional<int32_t> FindMatchingColumn(const std::function<bool(const std::string&)> predicate) const;

 private:
  int32_t schema_id_;
  std::vector<types::NestedField> fields_;
};

}  // namespace iceberg
