#pragma once

#include <memory>
#include <string>

#include "parquet/arrow/reader.h"

namespace iceberg {

class IFileReaderProvider {
 public:
  virtual arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> Open(const std::string& url) const = 0;

  virtual ~IFileReaderProvider() = default;
};

}  // namespace iceberg
