#include <arrow/filesystem/localfs.h>
#include <arrow/status.h>

#include <filesystem>
#include <fstream>
#include <ios>
#include <vector>

#include "gtest/gtest.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/tea_scan.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

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

ScopedTempDir::ScopedTempDir() {
  std::string name_template{std::filesystem::temp_directory_path() / "iceberg_test_XXXXXX"};
  if (!mkdtemp(name_template.data())) std::abort();
  path_ = name_template;
}
ScopedTempDir::~ScopedTempDir() { std::filesystem::remove_all(path_); }

TEST(ReferencedDataFile, ManifestEntry) {
  Manifest manifest;
  {
    ManifestEntry entry;
    entry.status = ManifestEntry::Status::kAdded;
    entry.data_file.content = ContentFile::FileContent::kPositionDeletes;
    entry.data_file.file_format = "PARQUET";
    entry.data_file.file_path = "s3://non-existing-delete-path";
    entry.data_file.record_count = 1;
    entry.data_file.file_size_in_bytes = 123;
    entry.data_file.referenced_data_file = "s3://non-existing-data-path";

    manifest.entries = {entry};
  }

  std::string data = ice_tea::WriteManifestEntries(manifest);
  auto result = ice_tea::ReadManifestEntries(data);

  ASSERT_EQ(result.entries.size(), 1);
  ASSERT_TRUE(result.entries[0].data_file.referenced_data_file.has_value());
  EXPECT_EQ(result.entries[0].data_file.referenced_data_file.value(), "s3://non-existing-data-path");
}

TEST(ReferencedDataFile, GetScanMetadata) {
  ScopedTempDir dir;
  {
    Manifest manifest;
    {
      ManifestEntry entry;
      entry.status = ManifestEntry::Status::kAdded;
      entry.data_file.content = ContentFile::FileContent::kPositionDeletes;
      entry.data_file.file_format = "PARQUET";
      entry.data_file.file_path = "s3://non-existing-delete-path";
      entry.data_file.record_count = 1;
      entry.data_file.file_size_in_bytes = 123;
      entry.data_file.referenced_data_file = "s3://non-existing-data-path";

      manifest.entries = {entry};
    }

    std::string data = ice_tea::WriteManifestEntries(manifest);
    auto result = ice_tea::ReadManifestEntries(data);
    std::ofstream f(dir.path() / "manifest.avro", std::ios_base::binary);
    f.write(data.data(), data.size());
    f.close();
  }

  {
    ManifestFile manifest_file;
    manifest_file.added_files_count = 1;
    manifest_file.added_rows_count = 1;
    manifest_file.partition_spec_id = 0;
    manifest_file.path = "file://" + (dir.path() / "manifest.avro").string();

    std::string data = ice_tea::WriteManifestList({manifest_file});

    std::ofstream f(dir.path() / "manifest_list.avro", std::ios_base::binary);
    f.write(data.data(), data.size());
    f.close();
  }

  {
    TableMetadataV2Builder builder;
    builder.table_uuid = "q";
    builder.location = "loc";
    builder.last_sequence_number = 1;
    builder.last_updated_ms = 1;
    builder.last_column_id = 10;
    builder.schemas = {
        std::make_shared<iceberg::Schema>(1, std::vector<types::NestedField>{types::NestedField{
                                                 .name = "c1",
                                                 .field_id = 1,
                                                 .is_required = false,
                                                 .type = std::make_shared<types::PrimitiveType>(TypeID::kInt)}})};
    builder.current_schema_id = 1;
    builder.partition_specs = {
        std::make_shared<iceberg::PartitionSpec>(iceberg::PartitionSpec{.spec_id = 1, .fields = {}})};
    builder.default_spec_id = 1;
    builder.last_partition_id = 1;
    builder.sort_orders = {std::make_shared<iceberg::SortOrder>(SortOrder{.order_id = 1, .fields = {}})};
    builder.default_sort_order_id = 1;
    builder.snapshots = {std::make_shared<Snapshot>(
        Snapshot{.snapshot_id = 1,
                 .parent_snapshot_id = std::nullopt,
                 .sequence_number = 1,
                 .timestamp_ms = 1,
                 .manifest_list_location = "file://" + (dir.path() / "manifest_list.avro").string(),
                 .summary = std::map<std::string, std::string>{{"operation", "append"}},
                 .schema_id = 1})};
    builder.current_snapshot_id = 1;
    auto table_metadata = builder.Build();
    ASSERT_TRUE(table_metadata != nullptr);

    std::string data = ice_tea::WriteTableMetadataV2(*table_metadata);

    std::ofstream f(dir.path() / "snapshot.json", std::ios_base::binary);
    f.write(data.data(), data.size());
    f.close();
  }

  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

  std::string snapshot_path = "file://" + (dir.path() / "snapshot.json").string();

  auto s = ice_tea::GetScanMetadata(fs, snapshot_path);

  ASSERT_FALSE(s.ok());
  EXPECT_EQ(s.status().message(), std::string("Referenced data file for delete files is not supported yet"));

// TODO(gmusya)
#if 0
  ASSERT_EQ(s.status(), arrow::Status::OK());
#endif
}

}  // namespace
}  // namespace iceberg
