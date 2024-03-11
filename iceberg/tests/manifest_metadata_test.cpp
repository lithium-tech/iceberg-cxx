#include "iceberg/src/manifest_metadata.h"

#include <fstream>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

namespace iceberg {

TEST(Manifest, SanityCheck) {
  std::ifstream input(
      "data/"
      "snap-7635660646343998149-1-10eaca8a-1e1c-421e-ad6d-b232e5ee23d3.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string manifest_data = ss.str();
  std::vector<ManifestMetadata> manifest_list = MakeManifestList(manifest_data);
  EXPECT_EQ(manifest_list.size(), 2);
  EXPECT_EQ(
      manifest_list[0].manifest_path,
      "lineitem_iceberg/metadata/10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m1.avro");
  EXPECT_EQ(manifest_list[0].manifest_length, 7692);
  EXPECT_EQ(manifest_list[0].partition_spec_id, 0);
  EXPECT_EQ(manifest_list[0].content_type, ContentType::kData);
  EXPECT_EQ(manifest_list[0].sequence_number, 2);
  EXPECT_EQ(manifest_list[0].min_sequence_number, 2);
  EXPECT_EQ(manifest_list[0].added_snapshot_id, 7635660646343998149);
  EXPECT_EQ(manifest_list[0].added_data_files_count, 1);
  EXPECT_EQ(manifest_list[0].existing_data_files_count, 0);
  EXPECT_EQ(manifest_list[0].deleted_data_files_count, 0);
  EXPECT_EQ(manifest_list[0].added_rows_count, 51793);
  EXPECT_EQ(manifest_list[0].existing_rows_count, 0);
  EXPECT_EQ(manifest_list[0].deleted_rows_count, 0);
  EXPECT_EQ(
      manifest_list[1].manifest_path,
      "lineitem_iceberg/metadata/10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m0.avro");
  EXPECT_EQ(manifest_list[1].manifest_length, 7687);
  EXPECT_EQ(manifest_list[1].partition_spec_id, 0);
  EXPECT_EQ(manifest_list[1].content_type, ContentType::kData);
  EXPECT_EQ(manifest_list[1].sequence_number, 2);
  EXPECT_EQ(manifest_list[1].min_sequence_number, 2);
  EXPECT_EQ(manifest_list[1].added_snapshot_id, 7635660646343998149);
  EXPECT_EQ(manifest_list[1].added_data_files_count, 0);
  EXPECT_EQ(manifest_list[1].existing_data_files_count, 0);
  EXPECT_EQ(manifest_list[1].deleted_data_files_count, 1);
  EXPECT_EQ(manifest_list[1].added_rows_count, 0);
  EXPECT_EQ(manifest_list[1].existing_rows_count, 0);
  EXPECT_EQ(manifest_list[1].deleted_rows_count, 60175);
}

}  // namespace iceberg
