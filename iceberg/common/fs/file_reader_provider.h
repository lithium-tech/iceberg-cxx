#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/result.h"
#include "iceberg/deletion_vector.h"
#include "parquet/arrow/reader.h"

namespace iceberg {

class IFileReaderProvider {
 public:
  virtual arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> OpenParquet(const std::string& url) const = 0;
  virtual arrow::Result<std::shared_ptr<DeletionVector>> OpenDeletionVector(const std::string& path, int64_t offset,
                                                                            int64_t length) const = 0;

  virtual ~IFileReaderProvider() = default;
};

}  // namespace iceberg
