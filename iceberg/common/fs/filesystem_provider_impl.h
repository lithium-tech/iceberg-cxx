#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/result.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/common/fs/s3.h"
#include "iceberg/common/fs/url.h"

namespace iceberg {

class IFileSystemGetter {
 public:
  virtual arrow::Result<std::shared_ptr<arrow::fs::FileSystem>> Get() = 0;

  virtual ~IFileSystemGetter() = default;
};

class FileSystemProvider : public IFileSystemProvider {
 public:
  explicit FileSystemProvider(std::map<std::string, std::shared_ptr<IFileSystemGetter>> schema_to_fs_builder)
      : schema_to_fs_builder_(std::move(schema_to_fs_builder)) {}

  arrow::Result<std::shared_ptr<arrow::fs::FileSystem>> GetFileSystem(const std::string& url) {
    auto components = SplitUrl(url);
    const std::string schema = std::string(components.schema);

    auto it = schema_to_fs_builder_.find(schema);
    if (it == schema_to_fs_builder_.end()) {
      return arrow::Status::ExecutionError("FileSystemProvider: unexpected filesystem schema ", schema, " for url ",
                                           url);
    }

    return it->second->Get();
  }

 private:
  std::map<std::string, std::shared_ptr<IFileSystemGetter>> schema_to_fs_builder_;
};

// TODO(gmusya): maybe move to distinct file
class S3FileSystemGetter : public IFileSystemGetter {
 public:
  struct Config {
    std::string endpoint_override;
    std::string scheme;
    std::string region;
    std::string access_key;
    std::string secret_key;
    std::chrono::milliseconds connect_timeout;
    std::chrono::milliseconds request_timeout;
    uint32_t retry_max_attempts;
  };

  explicit S3FileSystemGetter(const Config& config) : config_(config) {}

  arrow::Result<std::shared_ptr<arrow::fs::FileSystem>> Get() override {
    if (!s3fs_) {
      ARROW_ASSIGN_OR_RAISE(auto options, MakeS3Options());
      retry_strategy_ = std::make_shared<CountingS3RetryStrategy>(config_.retry_max_attempts);
      ARROW_ASSIGN_OR_RAISE(s3fs_, MakeS3FileSystem(options, retry_strategy_));
    }
    return s3fs_;
  }

 private:
  arrow::Result<arrow::fs::S3Options> MakeS3Options() {
    ARROW_RETURN_NOT_OK(InitializeS3IfNecessary());

    auto options = arrow::fs::S3Options::FromAccessKey(config_.access_key, config_.secret_key);
    options.endpoint_override = config_.endpoint_override;
    options.scheme = config_.scheme;
    options.region = config_.region;
    options.connect_timeout = config_.connect_timeout.count() / 1000.0;
    options.request_timeout = config_.request_timeout.count() / 1000.0;
    return options;
  }

  static arrow::Status InitializeS3IfNecessary() {
    if (!arrow::fs::IsS3Initialized()) {
      arrow::fs::S3GlobalOptions global_options{};
      global_options.log_level = arrow::fs::S3LogLevel::Fatal;
      return arrow::fs::InitializeS3(global_options);
    }
    return arrow::Status::OK();
  }

  Config config_;

  std::shared_ptr<CountingS3RetryStrategy> retry_strategy_;
  std::shared_ptr<arrow::fs::FileSystem> s3fs_;
};

class LocalFileSystemGetter : public IFileSystemGetter {
 public:
  arrow::Result<std::shared_ptr<arrow::fs::FileSystem>> Get() override {
    if (!localfs_) {
      localfs_ = std::make_shared<arrow::fs::LocalFileSystem>();
    }
    return localfs_;
  }

 private:
  std::shared_ptr<arrow::fs::FileSystem> localfs_;
};

class SlowFileSystemGetter : public IFileSystemGetter {
 public:
  explicit SlowFileSystemGetter(std::shared_ptr<IFileSystemGetter> getter, double average_latency_seconds)
      : getter_(getter), average_latency_seconds_(average_latency_seconds) {}

  arrow::Result<std::shared_ptr<arrow::fs::FileSystem>> Get() override {
    if (!fs_) {
      ARROW_ASSIGN_OR_RAISE(auto underlying_fs, getter_->Get());
      fs_ = std::make_shared<arrow::fs::SlowFileSystem>(underlying_fs, average_latency_seconds_);
    }
    return fs_;
  }

 private:
  std::shared_ptr<IFileSystemGetter> getter_;
  double average_latency_seconds_;

  std::shared_ptr<arrow::fs::FileSystem> fs_;
};

}  // namespace iceberg
