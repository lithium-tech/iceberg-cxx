#include "src/manifest_entry.h"

#include <fstream>
#include <sstream>

#include "gtest/gtest.h"

namespace iceberg {

static void Check(const std::vector<ManifestEntry>& entries) {
  EXPECT_EQ(entries.size(), 6);
  const auto& entry = entries[0];
  EXPECT_EQ(entry.status, ManifestEntry::Status::kAdded);
  EXPECT_EQ(entry.snapshot_id, 5231658854638766100);
  EXPECT_EQ(entry.sequence_number, std::nullopt);
  EXPECT_EQ(entry.file_sequence_number, std::nullopt);
  const auto& data_file = entry.data_file;
  EXPECT_EQ(data_file.content, DataFile::FileContent::kData);
  EXPECT_EQ(data_file.file_path,
            "s3://warehouse/gperov/test/data/00000-6-d4e36f4d-a2c0-467d-90e7-0ef1a54e2724-0-00001.parquet");
  EXPECT_EQ(data_file.file_format, "PARQUET");
  EXPECT_EQ(data_file.record_count, 1024);
  EXPECT_EQ(data_file.file_size_in_bytes, 3980);
  std::map<int32_t, int64_t> expected_column_sizes = {{1, 1715}, {2, 1673}};
  EXPECT_EQ(data_file.column_sizes, expected_column_sizes);
  std::map<int32_t, int64_t> expected_value_counts{{1, 1024}, {2, 1024}};
  EXPECT_EQ(data_file.value_counts, expected_value_counts);
  std::vector<int64_t> expected_split_offsets = {4};
  EXPECT_EQ(data_file.split_offsets, expected_split_offsets);
  EXPECT_EQ(data_file.equality_ids.size(), 0);
  std::map<int32_t, std::vector<uint8_t>> expected_lower_bounds{{1, {0, 0, 0, 0, 0, 0, 0, 0}},
                                                                {2, {0, 0, 0, 0, 0, 0, 0, 0}}};
  EXPECT_EQ(data_file.lower_bounds, expected_lower_bounds);
  std::map<int32_t, std::vector<uint8_t>> expected_upper_bounds{{1, {255, 3, 0, 0, 0, 0, 0, 0}},
                                                                {2, {254, 7, 0, 0, 0, 0, 0, 0}}};
  EXPECT_EQ(data_file.upper_bounds, expected_upper_bounds);
  std::map<int32_t, int64_t> expected_null_value_counts = {{1, 0}, {2, 0}};
  EXPECT_EQ(data_file.null_value_counts, expected_null_value_counts);
  std::map<int32_t, int64_t> expected_nan_value_counts = {};
  EXPECT_EQ(data_file.nan_value_counts, expected_nan_value_counts);
  std::vector<uint8_t> expected_key_metadata = {};
  EXPECT_EQ(data_file.key_metadata.size(), 0);
  EXPECT_EQ(data_file.sort_order_id, 0);
  EXPECT_EQ(data_file.distinct_counts.size(), 0);
}

TEST(ManifestEntryTest, Test) {
  std::ifstream input("metadata/7e6e13cb-31fd-4de7-8811-02ce7cec44a9-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ManifestEntry> entries = ice_tea::ReadManifestEntries(data);
  Check(entries);
}

TEST(ManifestEntryTest, ReadWriteRead) {
  std::ifstream input("metadata/7e6e13cb-31fd-4de7-8811-02ce7cec44a9-m0.avro");

  std::vector<ManifestEntry> entries = ice_tea::ReadManifestEntries(input);
  Check(entries);

  std::string serialized = ice_tea::WriteManifestEntries(entries);
  entries = ice_tea::ReadManifestEntries(serialized);
  Check(entries);
}

TEST(ManifestEntryTest, Test2) {
  std::ifstream input("metadata/41f34bc8-eedf-4573-96b0-10c04e7c84c4-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ManifestEntry> entries = ice_tea::ReadManifestEntries(data);
  EXPECT_EQ(entries.size(), 1);
  const auto& entry = entries[0];
  EXPECT_EQ(entry.status, ManifestEntry::Status::kAdded);
  EXPECT_EQ(entry.snapshot_id, 7558608030923099867);
  EXPECT_EQ(entry.sequence_number, std::nullopt);
  EXPECT_EQ(entry.file_sequence_number, std::nullopt);
  const auto& data_file = entry.data_file;
  EXPECT_EQ(data_file.content, DataFile::FileContent::kPositionDeletes);
  EXPECT_EQ(data_file.file_path,
            "s3://warehouse/gperov/test/data/00000-13-85b2f39e-780b-4214-912b-df665f506333-00001-deletes.parquet");
  EXPECT_EQ(data_file.file_format, "PARQUET");
  EXPECT_EQ(data_file.record_count, 1);
  EXPECT_EQ(data_file.file_size_in_bytes, 1391);
  std::map<int32_t, int64_t> expected_column_sizes = {{2147483546, 121}, {2147483545, 40}};
  EXPECT_EQ(data_file.column_sizes, expected_column_sizes);
  EXPECT_EQ(data_file.value_counts.size(), 0);
  std::vector<int64_t> expected_split_offsets = {4};
  EXPECT_EQ(data_file.split_offsets, expected_split_offsets);
  EXPECT_EQ(data_file.equality_ids.size(), 0);
}

}  // namespace iceberg
