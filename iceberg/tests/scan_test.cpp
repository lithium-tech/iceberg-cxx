#include "iceberg/src/scan.h"

#include <algorithm>
#include <exception>
#include <string>
#include <tuple>
#include <vector>

#include "arrow/filesystem/s3fs.h"
#include "arrow/status.h"
#include "gtest/gtest.h"
#include "iceberg/src/hive_client.h"
#include "iceberg/tests/src/config.h"

namespace iceberg {

namespace {

void SortEntries(std::vector<ManifestEntry>& entries) {
  std::sort(entries.begin(), entries.end(),
            [](const auto& lhs, const auto& rhs) {
              const auto& d_lhs = lhs.data_file;
              const auto& d_rhs = rhs.data_file;
              return std::tie(lhs.sequence_number.value(), d_lhs.file_path) <
                     std::tie(rhs.sequence_number.value(), d_rhs.file_path);
            });
}

arrow::Result<Config> MakeDefaultConfig() {
  Config config;
  ARROW_ASSIGN_OR_RAISE(auto config_file_path, Config::GetFilePath());
  ARROW_RETURN_NOT_OK(config.FromFile(config_file_path));
  return config;
}

arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>> MakeS3FileSystem(
    const S3Config& config) {
  if (!arrow::fs::IsS3Initialized()) {
    arrow::fs::S3GlobalOptions global_options{};
    global_options.log_level = arrow::fs::S3LogLevel::Fatal;
    ARROW_RETURN_NOT_OK(arrow::fs::InitializeS3(global_options));
  }

  auto options =
      arrow::fs::S3Options::FromAccessKey(config.access_key, config.secret_key);
  options.endpoint_override = config.endpoint_override;
  options.scheme = config.scheme;
  options.region = config.region;
  options.connect_timeout = config.connect_timeout.count() * 1000;
  options.request_timeout = config.request_timeout.count() * 1000;
  return arrow::fs::S3FileSystem::Make(options);
}

}  // namespace

// TODO(gmusya)
#ifdef ICEBERG_LOCAL_TESTS
TEST(Scan, Test) {
  auto maybe_config = MakeDefaultConfig();
  ASSERT_EQ(maybe_config.status(), arrow::Status::OK());
  auto config = maybe_config.ValueUnsafe();
  auto s3fs = MakeS3FileSystem(config.s3);
  ASSERT_TRUE(s3fs.ok());
  auto fs = s3fs.ValueUnsafe();
  HiveClient hive_client("localhost", 9083);
  auto maybe_scan_metadata =
      GetScanMetadata("test_wrk", "ashv_test3", fs, hive_client);
  ASSERT_TRUE(maybe_scan_metadata.ok());
  auto entries = maybe_scan_metadata.ValueUnsafe().entries;
  auto schema = maybe_scan_metadata.ValueUnsafe().schema;
  SortEntries(entries);
  ASSERT_EQ(entries.size(), 6);
  {
    EXPECT_EQ(entries[0].data_file.file_size_in_bytes, 724);
    EXPECT_EQ(entries[0].data_file.file_path,
              "s3a://iceberg.test-wrk.ashv-test3/ashv_test3/full.612/"
              "out_0_0.parquet");
  }
  {
    EXPECT_EQ(entries[3].data_file.file_size_in_bytes, 295);
    EXPECT_EQ(entries[3].data_file.file_path,
              "s3a://iceberg.test-wrk.ashv-test3/ashv_test3/diff.613/"
              "out_0_0_DELETE.parquet");
  }
  {
    EXPECT_EQ(entries[4].data_file.file_size_in_bytes, 977);
    EXPECT_EQ(entries[4].data_file.file_path,
              "s3a://iceberg.test-wrk.ashv-test3/ashv_test3/diff.613/"
              "out_1_0_DATA.parquet");
  }

  EXPECT_EQ(schema.GetSchemaId(), 0);
  EXPECT_EQ(schema.GetFields().size(), 3);

  ASSERT_EQ(arrow::fs::FinalizeS3(), arrow::Status::OK());
}
#endif

}  // namespace iceberg
