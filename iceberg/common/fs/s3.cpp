#include "iceberg/common/fs/s3.h"

namespace iceberg {

CountingS3RetryStrategy::CountingS3RetryStrategy(int64_t max_attempts)
    : s3_retry_strategy_(arrow::fs::S3RetryStrategy::GetAwsStandardRetryStrategy(max_attempts)) {}

bool CountingS3RetryStrategy::ShouldRetry(const AWSErrorDetail& error, int64_t attempted_retries) {
  bool result = s3_retry_strategy_->ShouldRetry(error, attempted_retries);
  if (result) {
    ++retry_count;
  }
  return result;
}

int64_t CountingS3RetryStrategy::CalculateDelayBeforeNextRetry(const AWSErrorDetail& error, int64_t attempted_retries) {
  return s3_retry_strategy_->CalculateDelayBeforeNextRetry(error, attempted_retries);
}

int64_t CountingS3RetryStrategy::GetRetryCount() const { return retry_count; }

arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>> MakeS3FileSystem(
    arrow::fs::S3Options options, std::shared_ptr<CountingS3RetryStrategy> retry_strategy) {
  setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
  options.retry_strategy = retry_strategy;
  return arrow::fs::S3FileSystem::Make(options);
}

}  // namespace iceberg
