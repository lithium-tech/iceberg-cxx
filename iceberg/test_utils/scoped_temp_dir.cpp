#include "iceberg/test_utils/scoped_temp_dir.h"

#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <string>

#include "arrow/filesystem/s3fs.h"

namespace iceberg {

ScopedTempDir::ScopedTempDir() {
  std::string name_template{std::filesystem::temp_directory_path() / "tea_smoke_test_XXXXXX"};
  if (!mkdtemp(name_template.data())) std::abort();
  path_ = name_template;
}
ScopedTempDir::~ScopedTempDir() { std::filesystem::remove_all(path_); }

ScopedS3TempDir::ScopedS3TempDir(std::shared_ptr<arrow::fs::S3FileSystem> fs) : fs_(fs) {
  std::mt19937 rnd(std::chrono::system_clock::now().time_since_epoch().count());
  std::string name;
  for (size_t i = 0; i < 6; ++i) {
    name += (rnd() % 26) + 'a';
  }
  auto status = fs->CreateDir(name);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    throw status;
  }

  path_ = name;
}

ScopedS3TempDir::~ScopedS3TempDir() {
  auto status = fs_->DeleteDirContents(std::string(path_).substr(5), true);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
  }
}

}  // namespace iceberg
