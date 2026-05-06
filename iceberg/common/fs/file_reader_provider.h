#pragma once

#include <memory>
#include <string>

#include "arrow/io/interfaces.h"
#include "parquet/arrow/reader.h"

namespace iceberg {

class IFileReaderProvider {
 public:
  virtual arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> Open(const std::string& url) const = 0;
  virtual arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenRaw(const std::string& url) const = 0;

  virtual ~IFileReaderProvider() = default;
};

}  // namespace iceberg
