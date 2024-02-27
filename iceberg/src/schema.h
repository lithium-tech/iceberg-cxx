#pragma once

#include <utility>
#include <vector>

#include "iceberg/src/field.h"

namespace iceberg {

class Schema {
 public:
  Schema(int32_t schema_id, std::vector<Field> fields)
      : schema_id_(schema_id), fields_(std::move(fields)) {}

  int32_t GetSchemaId() const { return schema_id_; }
  const std::vector<Field>& GetFields() const { return fields_; }

 private:
  int32_t schema_id_;
  std::vector<Field> fields_;
};

}  // namespace iceberg
