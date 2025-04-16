#pragma once

#include <memory>
#include <string>

#include "arrow/filesystem/filesystem.h"
#include "arrow/result.h"

namespace iceberg {

class IFileSystemProvider {
 public:
  virtual arrow::Result<std::shared_ptr<arrow::fs::FileSystem>> GetFileSystem(const std::string& url) = 0;

  virtual ~IFileSystemProvider() = default;
};

}  // namespace iceberg
