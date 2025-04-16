#include "iceberg/equality_delete/common.h"

namespace iceberg {

int TypeToBitWidth(arrow::Type::type type) {
  switch (type) {
    case arrow::Type::INT16:
      return 16;
    case arrow::Type::INT32:
      return 32;
    case arrow::Type::INT64:
      return 64;
    case arrow::Type::TIMESTAMP:
      return 64;
    default:
      throw arrow::Status::ExecutionError("TypeToBitWidth: unsupported type");
  }
}

}  // namespace iceberg
