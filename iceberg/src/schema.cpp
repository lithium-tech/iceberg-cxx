#include "iceberg/src/schema.h"

namespace iceberg {

std::optional<int32_t> Schema::FindMatchingColumn(const std::function<bool(const std::string&)> predicate) const {
  for (const auto& field : fields_) {
    if (predicate(field.name)) {
      return field.field_id;
    }
  }
  return std::nullopt;
}

}  // namespace iceberg
