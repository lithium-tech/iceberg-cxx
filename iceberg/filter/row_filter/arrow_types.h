#pragma once

#include <memory>

#include "arrow/type_fwd.h"

namespace iceberg {

std::shared_ptr<arrow::DataType> ArrowTimestampType();
std::shared_ptr<arrow::DataType> ArrowTimestamptzType();

}  // namespace iceberg
