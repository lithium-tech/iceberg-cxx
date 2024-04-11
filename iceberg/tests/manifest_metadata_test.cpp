#include <fstream>
#include <sstream>
#include <string>

#include "gtest/gtest.h"
#include "iceberg/src/manifest_file.h"

namespace iceberg {

TEST(Manifest, SanityCheck) {
  std::ifstream input(
      "metadata/"
      "snap-765518724043979080-1-5c8077bc-bb60-406d-ace2-586694e7ebea.avro");

  std::stringstream ss;
  ss << input.rdbuf();
  std::string manifest_data = ss.str();
  std::vector<ManifestFile> manifest_list = ice_tea::MakeManifestList(manifest_data);
  EXPECT_EQ(manifest_list.size(), 2);
  EXPECT_EQ(manifest_list[0].path,
            "s3://warehouse/gperov/test/metadata/"
            "0c0f3dbb-cb29-488b-8c01-368366432478-m0.avro");
  EXPECT_EQ(manifest_list[0].length, 6824);
  EXPECT_EQ(manifest_list[0].partition_spec_id, 0);
  EXPECT_EQ(manifest_list[0].content, ManifestContent::kData);
  EXPECT_EQ(manifest_list[0].sequence_number, 2);
  EXPECT_EQ(manifest_list[0].min_sequence_number, 2);
  EXPECT_EQ(manifest_list[0].snapshot_id, 2635333433439510679);
  EXPECT_EQ(manifest_list[0].added_files_count, 6);
  EXPECT_EQ(manifest_list[0].existing_files_count, 0);
  EXPECT_EQ(manifest_list[0].deleted_files_count, 0);
  EXPECT_EQ(manifest_list[0].added_rows_count, 10000);
  EXPECT_EQ(manifest_list[0].existing_rows_count, 0);
  EXPECT_EQ(manifest_list[0].deleted_rows_count, 0);
  EXPECT_EQ(manifest_list[1].path,
            "s3://warehouse/gperov/test/metadata/"
            "5c8077bc-bb60-406d-ace2-586694e7ebea-m0.avro");
  EXPECT_EQ(manifest_list[1].length, 6712);
  EXPECT_EQ(manifest_list[1].partition_spec_id, 0);
  EXPECT_EQ(manifest_list[1].content, ManifestContent::kDeletes);
  EXPECT_EQ(manifest_list[1].sequence_number, 3);
  EXPECT_EQ(manifest_list[1].min_sequence_number, 3);
  EXPECT_EQ(manifest_list[1].snapshot_id, 765518724043979080);
  EXPECT_EQ(manifest_list[1].added_files_count, 1);
  EXPECT_EQ(manifest_list[1].existing_files_count, 0);
  EXPECT_EQ(manifest_list[1].deleted_files_count, 0);
  EXPECT_EQ(manifest_list[1].added_rows_count, 1);
  EXPECT_EQ(manifest_list[1].existing_rows_count, 0);
  EXPECT_EQ(manifest_list[1].deleted_rows_count, 0);
}

}  // namespace iceberg
