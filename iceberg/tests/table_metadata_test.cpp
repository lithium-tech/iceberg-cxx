#include "iceberg/src/table_metadata.h"

#include <fstream>

#include "gtest/gtest.h"
#include "iceberg/src/type.h"

namespace iceberg {

TEST(Metadata, SanityCheck) {
  std::ifstream input("metadata/00003-aaa5649c-d0a0-4bdd-bf89-1a63bba01b37.metadata.json");

  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  auto metadata_ptr = MakeTableMetadataV2(data);
  const TableMetadataV2& metadata = *metadata_ptr;
  EXPECT_EQ(metadata.location, "s3://warehouse/gperov/test");
  EXPECT_EQ(metadata.table_uuid, "83d399af-a71f-4a31-8625-a39c7e97953e");
  EXPECT_EQ(metadata.last_sequence_number, 3);
  EXPECT_EQ(metadata.last_updated_ms, 1710939086483);
  EXPECT_EQ(metadata.last_column_id, 2);
  EXPECT_EQ(metadata.schemas.size(), 1);
  EXPECT_EQ(metadata.schemas[0]->SchemaId(), 0);
  ASSERT_EQ(metadata.schemas[0]->Columns().size(), 2);
  auto& field = metadata.schemas[0]->Columns()[1];
  EXPECT_EQ(field.field_id, 2);
  EXPECT_EQ(field.is_required, false);
  EXPECT_EQ(field.name, "b");
  ASSERT_EQ(field.type->IsPrimitiveType(), true);
  EXPECT_EQ(field.type->TypeId(), iceberg::TypeID::kLong);
  EXPECT_EQ(metadata.current_schema_id, 0);
  EXPECT_EQ(metadata.default_spec_id, 0);
  EXPECT_EQ(metadata.last_partition_id, 999);
  std::map<std::string, std::string> expected_properties = {{"created-at", "2024-03-20T12:47:31.109851401Z"},
                                                            {"owner", "root"},
                                                            {"write.delete.mode", "merge-on-read"},
                                                            {"write.parquet.compression-codec", "zstd"}};
  EXPECT_EQ(metadata.properties, expected_properties);
  EXPECT_EQ(metadata.current_snapshot_id, 765518724043979080);
  EXPECT_TRUE(!metadata.snapshots.empty());
  const auto& snapshots = metadata.snapshots;
  EXPECT_EQ(snapshots.size(), 3);
  EXPECT_EQ(snapshots[1]->snapshot_id, 2635333433439510679);
  EXPECT_EQ(snapshots[1]->parent_snapshot_id.has_value(), true);
  EXPECT_EQ(snapshots[1]->sequence_number, 2);
  EXPECT_EQ(snapshots[1]->timestamp_ms, 1710938891838);
  std::map<std::string, std::string> expected_summary_for_snapshot_0 = {
      {"added-data-files", "6"},        {"added-files-size", "25206"},   {"added-records", "10000"},
      {"changed-partition-count", "1"}, {"operation", "append"},         {"spark.app.id", "local-1710938745155"},
      {"total-data-files", "6"},        {"total-delete-files", "0"},     {"total-equality-deletes", "0"},
      {"total-files-size", "25206"},    {"total-position-deletes", "0"}, {"total-records", "10000"}};
  EXPECT_EQ(snapshots[1]->summary, expected_summary_for_snapshot_0);
  EXPECT_EQ(snapshots[2]->schema_id, 0);
  EXPECT_EQ(snapshots[2]->snapshot_id, 765518724043979080);
  EXPECT_EQ(snapshots[2]->parent_snapshot_id, 2635333433439510679);
  EXPECT_EQ(snapshots[2]->sequence_number, 3);
  EXPECT_EQ(snapshots[2]->timestamp_ms, 1710939086483);
  std::map<std::string, std::string> expected_summary_for_snapshot_1 = {{"added-delete-files", "1"},
                                                                        {"added-files-size", "1393"},
                                                                        {"added-position-delete-files", "1"},
                                                                        {"added-position-deletes", "1"},
                                                                        {"changed-partition-count", "1"},
                                                                        {"operation", "overwrite"},
                                                                        {"spark.app.id", "local-1710938745155"},
                                                                        {"total-data-files", "6"},
                                                                        {"total-delete-files", "1"},
                                                                        {"total-equality-deletes", "0"},
                                                                        {"total-files-size", "26599"},
                                                                        {"total-position-deletes", "1"},
                                                                        {"total-records", "10000"}};
  EXPECT_EQ(snapshots[2]->summary, expected_summary_for_snapshot_1);
  EXPECT_EQ(snapshots[2]->schema_id, 0);
  std::vector<std::pair<int64_t, int64_t>> expected_snapshot_log = {
      {1710938855718, 2084304437907955247}, {1710938891838, 2635333433439510679}, {1710939086483, 765518724043979080}};
  EXPECT_EQ(metadata.snapshot_log, expected_snapshot_log);
  std::vector<std::pair<int64_t, std::string>> expected_metadata_log = {
      {1710938855718,
       "s3://warehouse/gperov/test/metadata/"
       "00000-2ea03b9c-978d-416d-abd3-f1944ad92444.metadata.json"},
      {1710938891838,
       "s3://warehouse/gperov/test/metadata/"
       "00001-3b18f38f-545a-4a5b-a35c-a9bfbbbb9952.metadata.json"},
      {1710939079188,
       "s3://warehouse/gperov/test/metadata/"
       "00002-d98f22c9-60fb-4061-8af1-61037369648f.metadata.json"}};
  EXPECT_EQ(metadata.metadata_log, expected_metadata_log);
  EXPECT_EQ(metadata.default_sort_order_id, 0);
}

}  // namespace iceberg
