#pragma once

#include <filesystem>
#include <memory>

#include "arrow/filesystem/s3fs.h"

namespace iceberg {
class ScopedTempDir {
 public:
  ScopedTempDir();
  ~ScopedTempDir();

  ScopedTempDir(const ScopedTempDir&) = delete;
  ScopedTempDir& operator=(const ScopedTempDir&) = delete;

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

class ScopedS3TempDir {
 public:
  explicit ScopedS3TempDir(std::shared_ptr<arrow::fs::S3FileSystem> fs);
  ~ScopedS3TempDir();

  ScopedS3TempDir(const ScopedS3TempDir&) = delete;
  ScopedS3TempDir& operator=(const ScopedS3TempDir&) = delete;

  const std::filesystem::path& path() const { return path_; }

 private:
  std::shared_ptr<arrow::fs::S3FileSystem> fs_;
  std::filesystem::path path_;
};

}  // namespace iceberg
