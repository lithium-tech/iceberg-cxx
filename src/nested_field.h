#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "src/type.h"

namespace iceberg::types {

struct NestedField {
  std::string name;
  int32_t field_id;
  bool is_required = false;
  std::shared_ptr<const types::Type> type;
};

}  // namespace iceberg::types
