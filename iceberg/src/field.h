#pragma once

#include <cstdint>
#include <string>

#include "iceberg/src/type.h"

namespace iceberg {

struct Field {
  std::string name;
  int32_t id;
  bool is_required;
  std::shared_ptr<const DataType> type;
};

}  // namespace iceberg
