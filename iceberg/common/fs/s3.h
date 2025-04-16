#pragma once

#include <atomic>
#include <memory>

#include "arrow/filesystem/s3fs.h"
#include "arrow/result.h"

namespace iceberg {

class CountingS3RetryStrategy : public arrow::fs::S3RetryStrategy {
 public:
  explicit CountingS3RetryStrategy(int64_t max_attempts);

  bool ShouldRetry(const AWSErrorDetail& error, int64_t attempted_retries) override;

  int64_t CalculateDelayBeforeNextRetry(const AWSErrorDetail& error, int64_t attempted_retries) override;

  int64_t GetRetryCount() const;

 private:
  std::shared_ptr<S3RetryStrategy> s3_retry_strategy_;
  std::atomic<int64_t> retry_count = 0;
};

arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>> MakeS3FileSystem(
    arrow::fs::S3Options options, std::shared_ptr<CountingS3RetryStrategy> retry_strategy = nullptr);

}  // namespace iceberg
