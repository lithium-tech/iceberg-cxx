#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "iceberg/type.h"

namespace iceberg::types {

struct NestedField {
  std::string name;
  int32_t field_id;
  bool is_required = false;
  std::shared_ptr<const types::Type> type;

  bool operator==(const NestedField& other) const {
    auto left_type = type->ToString();
    auto right_type = other.type->ToString();

    return std::tie(name, field_id, is_required, left_type) ==
           std::tie(other.name, other.field_id, other.is_required, right_type);
  }
};

}  // namespace iceberg::types
