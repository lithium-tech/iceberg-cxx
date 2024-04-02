#include "iceberg/src/manifest_entry.h"

#include <fstream>
#include <sstream>

#include "gtest/gtest.h"

namespace iceberg {

TEST(ManifestEntryTest, Test) {
  std::ifstream input("metadata/0c0f3dbb-cb29-488b-8c01-368366432478-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ManifestEntry> entries = MakeManifestEntries(data);
  EXPECT_EQ(entries.size(), 6);
  const auto& entry = entries[0];
  EXPECT_EQ(entry.status, ManifestEntry::Status::kAdded);
  EXPECT_EQ(entry.snapshot_id, 2635333433439510679);
  EXPECT_EQ(entry.sequence_number, std::nullopt);
  EXPECT_EQ(entry.file_sequence_number, std::nullopt);
  const auto& data_file = entry.data_file;
  EXPECT_EQ(data_file.content, DataFile::Content::kData);
  EXPECT_EQ(data_file.file_path,
            "s3://warehouse/gperov/test/data/"
            "00000-6-9183b96d-8d9f-4514-b60d-1ea34766c578-0-00001.parquet");
  EXPECT_EQ(data_file.file_format, "PARQUET");
  EXPECT_EQ(data_file.record_count, 1024);
  EXPECT_EQ(data_file.file_size_in_bytes, 3980);
  std::vector<std::pair<int32_t, int64_t>> expected_column_sizes = {{1, 1715}, {2, 1673}};
  EXPECT_EQ(data_file.column_sizes, expected_column_sizes);
  std::vector<std::pair<int32_t, int64_t>> expected_value_counts{{1, 1024}, {2, 1024}};
  EXPECT_EQ(data_file.value_counts, expected_value_counts);
  std::vector<int64_t> expected_split_offsets = {4};
  EXPECT_EQ(data_file.split_offsets, expected_split_offsets);
  EXPECT_EQ(data_file.equality_ids, std::nullopt);
  std::vector<std::pair<int32_t, std::vector<uint8_t>>> expected_lower_bounds{{1, {0, 0, 0, 0, 0, 0, 0, 0}},
                                                                              {2, {0, 0, 0, 0, 0, 0, 0, 0}}};
  EXPECT_EQ(data_file.lower_bounds, expected_lower_bounds);
  std::vector<std::pair<int32_t, std::vector<uint8_t>>> expected_upper_bounds{{1, {255, 3, 0, 0, 0, 0, 0, 0}},
                                                                              {2, {254, 7, 0, 0, 0, 0, 0, 0}}};
  EXPECT_EQ(data_file.upper_bounds, expected_upper_bounds);
  std::vector<std::pair<int32_t, int64_t>> expected_null_value_counts = {{1, 0}, {2, 0}};
  EXPECT_EQ(data_file.null_value_counts, expected_null_value_counts);
  std::vector<std::pair<int32_t, int64_t>> expected_nan_value_counts = {};
  EXPECT_EQ(data_file.nan_value_counts, expected_nan_value_counts);
  std::vector<uint8_t> expected_key_metadata = {};
  EXPECT_EQ(data_file.key_metadata, std::nullopt);
  EXPECT_EQ(data_file.sort_order_id, 0);
  EXPECT_EQ(data_file.distinct_counts, std::nullopt);
}

TEST(ManifestEntryTest, Test2) {
  std::ifstream input("metadata/5c8077bc-bb60-406d-ace2-586694e7ebea-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ManifestEntry> entries = MakeManifestEntries(data);
  EXPECT_EQ(entries.size(), 1);
  const auto& entry = entries[0];
  EXPECT_EQ(entry.status, ManifestEntry::Status::kAdded);
  EXPECT_EQ(entry.snapshot_id, 765518724043979080);
  EXPECT_EQ(entry.sequence_number, std::nullopt);
  EXPECT_EQ(entry.file_sequence_number, std::nullopt);
  const auto& data_file = entry.data_file;
  EXPECT_EQ(data_file.content, DataFile::Content::kPositionDelete);
  EXPECT_EQ(data_file.file_path,
            "s3://warehouse/gperov/test/data/"
            "00000-13-dacb3d8d-55e9-45af-b186-ce208da1f36a-00001-deletes.parquet");
  EXPECT_EQ(data_file.file_format, "PARQUET");
  EXPECT_EQ(data_file.record_count, 1);
  EXPECT_EQ(data_file.file_size_in_bytes, 1393);
  std::vector<std::pair<int32_t, int64_t>> expected_column_sizes = {{2147483546, 123}, {2147483545, 40}};
  EXPECT_EQ(data_file.column_sizes, expected_column_sizes);
  EXPECT_EQ(data_file.value_counts, std::nullopt);
  std::vector<int64_t> expected_split_offsets = {4};
  EXPECT_EQ(data_file.split_offsets, expected_split_offsets);
  EXPECT_EQ(data_file.equality_ids, std::nullopt);
}

}  // namespace iceberg
