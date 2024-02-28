#include "iceberg/src/manifest_entry.h"

#include <fstream>
#include <sstream>

#include "gtest/gtest.h"

namespace iceberg {

TEST(ManifestEntryTest, Test) {
  std::ifstream input(
      "data/"
      "10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ManifestEntry> entries = MakeManifestEntries(data);
  EXPECT_EQ(entries.size(), 1);
  const auto& entry = entries[0];
  EXPECT_EQ(entry.status, ManifestEntry::Status::DELETED);
  EXPECT_EQ(entry.snapshot_id, 7635660646343998149);
  EXPECT_EQ(entry.sequence_number, std::nullopt);
  EXPECT_EQ(entry.file_sequence_number, std::nullopt);
  const auto& data_file = entry.data_file;
  EXPECT_EQ(data_file.content, DataFile::Content::DATA);
  EXPECT_EQ(data_file.file_path,
            "lineitem_iceberg/data/"
            "00000-411-0792dcfe-4e25-4ca3-8ada-175286069a47-00001.parquet");
  EXPECT_EQ(data_file.file_format, "PARQUET");
  EXPECT_EQ(data_file.record_count, 60175);
  EXPECT_EQ(data_file.file_size_in_bytes, 1390176);
  std::vector<std::pair<int32_t, int64_t>> expected_column_sizes = {
      {1, 84424},  {2, 87455},  {3, 53095},  {4, 10336},
      {5, 45294},  {6, 227221}, {7, 26671},  {8, 24651},
      {9, 10797},  {10, 6869},  {11, 95073}, {12, 94808},
      {13, 95153}, {14, 15499}, {15, 22569}, {16, 484914}};
  EXPECT_EQ(data_file.column_sizes, expected_column_sizes);
  std::vector<std::pair<int32_t, int64_t>> expected_value_counts{
      {1, 60175},  {2, 60175},  {3, 60175},  {4, 60175},
      {5, 60175},  {6, 60175},  {7, 60175},  {8, 60175},
      {9, 60175},  {10, 60175}, {11, 60175}, {12, 60175},
      {13, 60175}, {14, 60175}, {15, 60175}, {16, 60175}};
  EXPECT_EQ(data_file.value_counts, expected_value_counts);
  std::vector<int64_t> expected_split_offsets = {4};
  EXPECT_EQ(data_file.split_offsets, expected_split_offsets);
  EXPECT_EQ(data_file.equality_ids, std::nullopt);
}

TEST(ManifestEntryTest, Test2) {
  std::ifstream input(
      "data/"
      "10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m1.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ManifestEntry> entries = MakeManifestEntries(data);
  EXPECT_EQ(entries.size(), 1);
  const auto& entry = entries[0];
  EXPECT_EQ(entry.status, ManifestEntry::Status::ADDED);
  EXPECT_EQ(entry.snapshot_id, 7635660646343998149);
  EXPECT_EQ(entry.sequence_number, std::nullopt);
  EXPECT_EQ(entry.file_sequence_number, std::nullopt);
  const auto& data_file = entry.data_file;
  EXPECT_EQ(data_file.content, DataFile::Content::DATA);
  EXPECT_EQ(data_file.file_path,
            "lineitem_iceberg/data/"
            "00041-414-f3c73457-bbd6-4b92-9c15-17b241171b16-00001.parquet");
  EXPECT_EQ(data_file.file_format, "PARQUET");
  EXPECT_EQ(data_file.record_count, 51793);
  EXPECT_EQ(data_file.file_size_in_bytes, 1208539);
  std::vector<std::pair<int32_t, int64_t>> expected_column_sizes = {
      {1, 78864},  {2, 75856},  {3, 45721},  {4, 13215},
      {5, 38425},  {6, 194003}, {7, 22935},  {8, 21199},
      {9, 9492},   {10, 6178},  {11, 82582}, {12, 82308},
      {13, 82663}, {14, 13332}, {15, 19427}, {16, 417423}};
  EXPECT_EQ(data_file.column_sizes, expected_column_sizes);
  std::vector<std::pair<int32_t, int64_t>> expected_value_counts{
      {1, 51793},  {2, 51793},  {3, 51793},  {4, 51793},
      {5, 51793},  {6, 51793},  {7, 51793},  {8, 51793},
      {9, 51793},  {10, 51793}, {11, 51793}, {12, 51793},
      {13, 51793}, {14, 51793}, {15, 51793}, {16, 51793}};
  EXPECT_EQ(data_file.value_counts, expected_value_counts);
  std::vector<int64_t> expected_split_offsets = {4};
  EXPECT_EQ(data_file.split_offsets, expected_split_offsets);
  EXPECT_EQ(data_file.equality_ids, std::nullopt);
}

}  // namespace iceberg
