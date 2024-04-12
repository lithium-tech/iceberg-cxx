#include <algorithm>
#include <exception>
#include <map>
#include <string>
#include <tuple>
#include <vector>

#include "arrow/filesystem/s3fs.h"
#include "arrow/status.h"
#include "gtest/gtest.h"
#include "iceberg/src/tea_hive_catalog.h"
#include "iceberg/src/tea_scan.h"

namespace iceberg {

#ifdef USE_ICEBERG
namespace {

void SortEntries(std::vector<ManifestEntry>& entries) {
  std::sort(entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
    const auto& d_lhs = lhs.data_file;
    const auto& d_rhs = rhs.data_file;
    return std::tie(lhs.sequence_number.value(), d_lhs.file_path) <
           std::tie(rhs.sequence_number.value(), d_rhs.file_path);
  });
}

arrow::Status InitS3() {
  if (!arrow::fs::IsS3Initialized()) {
    arrow::fs::S3GlobalOptions global_options{};
    global_options.log_level = arrow::fs::S3LogLevel::Fatal;
    ARROW_RETURN_NOT_OK(arrow::fs::InitializeS3(global_options));
  }
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>> MakeS3FileSystem() {
  ARROW_RETURN_NOT_OK(InitS3());

  auto options = arrow::fs::S3Options::FromAccessKey("minioadmin", "minioadmin");
  options.endpoint_override = "127.0.0.1:9000";
  options.scheme = "http";
  return arrow::fs::S3FileSystem::Make(options);
}

}  // namespace

TEST(Scan, Test) {
  auto s3fs = MakeS3FileSystem();
  ASSERT_TRUE(s3fs.ok());
  auto fs = s3fs.ValueUnsafe();
  ice_tea::HiveCatalog hive_client("127.0.0.1", 9090);
  auto table = hive_client.LoadTable(catalog::TableIdentifier{.db = "gperov", .name = "test"});
  auto maybe_scan_metadata = ice_tea::GetScanMetadata(fs, table->Location());
  ASSERT_TRUE(maybe_scan_metadata.ok());
  auto entries = maybe_scan_metadata.ValueUnsafe().entries;
  auto schema = maybe_scan_metadata.ValueUnsafe().schema;
  SortEntries(entries);
  ASSERT_EQ(entries.size(), 7);
  {
    EXPECT_EQ(entries[0].data_file.file_size_in_bytes, 3980);
    EXPECT_EQ(entries[0].data_file.file_path,
              "s3://warehouse/gperov/test/data/"
              "00000-6-9183b96d-8d9f-4514-b60d-1ea34766c578-0-00001.parquet");
    EXPECT_EQ(entries[0].data_file.content, DataFile::FileContent::kData);
  }
  {
    EXPECT_EQ(entries[3].data_file.file_size_in_bytes, 2768);
    EXPECT_EQ(entries[3].data_file.file_path,
              "s3://warehouse/gperov/test/data/"
              "00003-9-9183b96d-8d9f-4514-b60d-1ea34766c578-0-00001.parquet");
    EXPECT_EQ(entries[3].data_file.content, DataFile::FileContent::kData);
  }
  {
    EXPECT_EQ(entries[6].data_file.file_size_in_bytes, 1393);
    EXPECT_EQ(entries[6].data_file.file_path,
              "s3://warehouse/gperov/test/data/"
              "00000-13-dacb3d8d-55e9-45af-b186-ce208da1f36a-00001-deletes."
              "parquet");
    EXPECT_EQ(entries[6].data_file.content, DataFile::FileContent::kPositionDeletes);
  }
  EXPECT_EQ(schema->SchemaId(), 0);
  EXPECT_EQ(schema->Columns().size(), 2);
}

TEST(Catalog, Test) {
  EXPECT_TRUE(InitS3().ok());

  std::map<std::string, std::string> properties = {{"s3.access_key", "minioadmin"},
                                                   {"s3.secret_key", "minioadmin"},
                                                   {"s3.endpoint_override", "127.0.0.1:9000"},
                                                   {"s3.scheme", "http"}};
  ice_tea::HiveCatalog hive_client("127.0.0.1", 9090);
  hive_client.Initialize("ice_tea", properties);
  auto table = hive_client.LoadTable(catalog::TableIdentifier{.db = "gperov", .name = "test"});
  auto location = table->Location();
  ASSERT_EQ(location, "s3://warehouse/gperov/test/metadata/00003-aaa5649c-d0a0-4bdd-bf89-1a63bba01b37.metadata.json");
  auto schema = table->GetSchema();
  EXPECT_TRUE(!!schema);
  EXPECT_EQ(schema->SchemaId(), 0);
  EXPECT_EQ(schema->Columns().size(), 2);
  EXPECT_EQ(schema->Columns()[0].name, "a");
  EXPECT_EQ(schema->Columns()[1].name, "b");
  EXPECT_TRUE(!!schema->Columns()[0].type);
  EXPECT_TRUE(!!schema->Columns()[1].type);
  EXPECT_EQ(schema->Columns()[0].type->ToString(), "long");
  EXPECT_EQ(schema->Columns()[1].type->ToString(), "long");
}
#endif

}  // namespace iceberg
