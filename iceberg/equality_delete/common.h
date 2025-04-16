#pragma once

#include <functional>
#include <memory>
#include <set>
#include <string>

#include "arrow/result.h"
#include "parquet/file_reader.h"

namespace iceberg {

using FieldId = int32_t;
using DeleteInfoId = uint32_t;

using ReaderMethodType =
    std::function<arrow::Result<std::shared_ptr<parquet::arrow::FileReader>>(const std::string& url)>;

int TypeToBitWidth(arrow::Type::type type);

}  // namespace iceberg
