#include "iceberg/filter/row_filter/arrow_types.h"

#include "arrow/type_fwd.h"

namespace iceberg {

std::shared_ptr<arrow::DataType> ArrowTimestampType() { return arrow::timestamp(arrow::TimeUnit::MICRO); }
std::shared_ptr<arrow::DataType> ArrowTimestamptzType() { return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"); }

}  // namespace iceberg
