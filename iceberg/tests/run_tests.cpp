#include <gtest/gtest.h>

#include "arrow/filesystem/s3fs.h"

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  arrow::fs::S3GlobalOptions global_options{};
  global_options.log_level = arrow::fs::S3LogLevel::Fatal;
  if (auto status = arrow::fs::InitializeS3(global_options); !status.ok()) {
    std::cerr << "Failed to initialize S3: " << status.message() << std::endl;
    return 1;
  }
  auto result = RUN_ALL_TESTS();
  if (auto status = arrow::fs::FinalizeS3(); !status.ok()) {
    std::cerr << "Failed to finalize S3: " << status.message() << std::endl;
    return 1;
  }
  return result;
}
