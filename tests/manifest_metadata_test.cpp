#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "gtest/gtest.h"
#include "iceberg/manifest_file.h"

namespace iceberg {
namespace {
std::vector<ManifestFile> expected_list = {
    ManifestFile{.added_files_count = 6,
                 .added_rows_count = 10000,
                 .content = ManifestContent::kData,
                 .deleted_files_count = 0,
                 .deleted_rows_count = 0,
                 .existing_files_count = 0,
                 .existing_rows_count = 0,
                 .length = 6825,
                 .min_sequence_number = 2,
                 .partition_spec_id = 0,
                 .path = "s3://warehouse/gperov/test/metadata/7e6e13cb-31fd-4de7-8811-02ce7cec44a9-m0.avro",
                 .sequence_number = 2,
                 .snapshot_id = 5231658854638766100},
    ManifestFile{.added_files_count = 1,
                 .added_rows_count = 1,
                 .content = ManifestContent::kDeletes,
                 .deleted_files_count = 0,
                 .deleted_rows_count = 0,
                 .existing_files_count = 0,
                 .existing_rows_count = 0,
                 .length = 6714,
                 .min_sequence_number = 3,
                 .partition_spec_id = 0,
                 .path = "s3://warehouse/gperov/test/metadata/41f34bc8-eedf-4573-96b0-10c04e7c84c4-m0.avro",
                 .sequence_number = 3,
                 .snapshot_id = 7558608030923099867}};

std::vector<ManifestFile> expected_list_uuid_generated = {
    ManifestFile{.added_files_count = 6,
                 .added_rows_count = 10000,
                 .content = ManifestContent::kData,
                 .deleted_files_count = 0,
                 .deleted_rows_count = 0,
                 .existing_files_count = 0,
                 .existing_rows_count = 0,
                 .length = 6825,
                 .min_sequence_number = 2,
                 .partition_spec_id = 0,
                 .path = "s3://warehouse/gperov/test/metadata/98887eae-200b-1006-9bb5-0242ac110002-m0.avro",
                 .sequence_number = 2,
                 .snapshot_id = 5231658854638766100},
    ManifestFile{.added_files_count = 1,
                 .added_rows_count = 1,
                 .content = ManifestContent::kDeletes,
                 .deleted_files_count = 0,
                 .deleted_rows_count = 0,
                 .existing_files_count = 0,
                 .existing_rows_count = 0,
                 .length = 6714,
                 .min_sequence_number = 3,
                 .partition_spec_id = 0,
                 .path = "s3://warehouse/gperov/test/metadata/98887ee4-200b-1006-9cd1-0242ac110002-m0.avro",
                 .sequence_number = 3,
                 .snapshot_id = 7558608030923099867}};

void CheckEqual(const ManifestFile& m1, const ManifestFile& m2) {
  EXPECT_EQ(m1.path, m2.path);
  EXPECT_EQ(m1.length, m2.length);
  EXPECT_EQ(m1.partition_spec_id, m2.partition_spec_id);
  EXPECT_EQ(m1.content, m2.content);
  EXPECT_EQ(m1.sequence_number, m2.sequence_number);
  EXPECT_EQ(m1.min_sequence_number, m2.min_sequence_number);
  EXPECT_EQ(m1.snapshot_id, m2.snapshot_id);
  EXPECT_EQ(m1.added_files_count, m2.added_files_count);
  EXPECT_EQ(m1.existing_files_count, m2.existing_files_count);
  EXPECT_EQ(m1.deleted_files_count, m2.deleted_files_count);
  EXPECT_EQ(m1.added_rows_count, m2.added_rows_count);
  EXPECT_EQ(m1.existing_rows_count, m2.existing_rows_count);
  EXPECT_EQ(m1.deleted_rows_count, m2.deleted_rows_count);
}
}  // namespace

TEST(Manifest, ReadSanityCheck) {
  std::ifstream input("metadata/snap-7558608030923099867-1-41f34bc8-eedf-4573-96b0-10c04e7c84c4.avro");

  std::vector<ManifestFile> manifest_list = ice_tea::ReadManifestList(input);
  EXPECT_EQ(manifest_list.size(), 2);
  CheckEqual(manifest_list[0], expected_list[0]);
  CheckEqual(manifest_list[1], expected_list[1]);
}

TEST(Manifest, ReadWriteRead) {
  std::ifstream input("metadata/snap-7558608030923099867-1-41f34bc8-eedf-4573-96b0-10c04e7c84c4.avro");

  std::vector<ManifestFile> manifest_list = ice_tea::ReadManifestList(input);
  auto manifest_data = ice_tea::WriteManifestList(manifest_list);
  manifest_list.clear();

  std::stringstream ss(manifest_data);
  manifest_list = ice_tea::ReadManifestList(ss);

  EXPECT_EQ(manifest_list.size(), 2);
  CheckEqual(manifest_list[0], expected_list[0]);
  CheckEqual(manifest_list[1], expected_list[1]);
}

TEST(Manifest, ReadFileUuidGenerator) {
  std::ifstream input("metadata/snap-2dfe8bdb-200b-1006-9bb5-0242ac110002.avro");

  std::vector<ManifestFile> manifest_list = ice_tea::ReadManifestList(input);
  EXPECT_EQ(manifest_list.size(), 2);
  CheckEqual(manifest_list[0], expected_list_uuid_generated[0]);
  CheckEqual(manifest_list[1], expected_list_uuid_generated[1]);
}

TEST(Manifest, ReadBrokenFiles) {
  std::ifstream input_empty("metadata/empty.avro");
  EXPECT_THROW(ice_tea::ReadManifestList(input_empty), std::runtime_error);

  std::ifstream input_broken1("metadata/broken1.avro");
  EXPECT_THROW(ice_tea::ReadManifestList(input_broken1), std::runtime_error);

  std::ifstream input_broken2("metadata/broken2.avro");
  EXPECT_THROW(ice_tea::ReadManifestList(input_broken2), std::runtime_error);

  std::ifstream input_ok("metadata/snap-2dfe8bdb-200b-1006-9bb5-0242ac110002.avro");
  EXPECT_NO_THROW(ice_tea::ReadManifestList(input_ok));
}

}  // namespace iceberg
