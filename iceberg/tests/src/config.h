#pragma once

#include <chrono>
#include <string>

#include "arrow/result.h"
#include "arrow/status.h"

namespace iceberg {

struct S3Config {
  std::string endpoint_override;
  std::string scheme;
  std::string region;
  std::string access_key;
  std::string secret_key;
  std::chrono::milliseconds connect_timeout = std::chrono::milliseconds::zero();
  std::chrono::milliseconds request_timeout = std::chrono::milliseconds::zero();
};

struct Config {
  S3Config s3;

  arrow::Status FromFile(const std::string& file_path);
  arrow::Status FromString(const std::string& content);
  static arrow::Result<std::string> GetFilePath();
  static const char* kFilePath;
};

}  // namespace iceberg
