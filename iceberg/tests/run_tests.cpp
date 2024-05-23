#include <gtest/gtest.h>

#include "arrow/filesystem/s3fs.h"
#include "iceberg/src/catalog.h"

namespace iceberg {

catalog::TableIdentifier GetTestTable() {
  catalog::TableIdentifier table_identifier{.db = "gperov", .name = "test"};
  if (auto* db = getenv("TEST_DB")) {
    table_identifier.db = db;
  }
  if (auto* table = getenv("TEST_TABLE")) {
    table_identifier.name = table;
  }
  return table_identifier;
}

}  // namespace iceberg

int main(int argc, char** argv) {
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
