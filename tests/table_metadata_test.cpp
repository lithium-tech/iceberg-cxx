#include "iceberg/table_metadata.h"

#include <fstream>

#include "gtest/gtest.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

std::map<std::string, std::string> expected_properties = {{"created-at", "2024-04-24T09:46:28.136251005Z"},
                                                          {"owner", "root"},
                                                          {"write.delete.mode", "merge-on-read"},
                                                          {"write.parquet.compression-codec", "zstd"}};

std::map<std::string, std::string> expected_summary_for_snapshot_0 = {
    {"added-data-files", "6"},        {"added-files-size", "25206"},   {"added-records", "10000"},
    {"changed-partition-count", "1"}, {"operation", "append"},         {"spark.app.id", "local-1713951981838"},
    {"total-data-files", "6"},        {"total-delete-files", "0"},     {"total-equality-deletes", "0"},
    {"total-files-size", "25206"},    {"total-position-deletes", "0"}, {"total-records", "10000"}};

std::map<std::string, std::string> expected_summary_for_snapshot_1 = {{"added-delete-files", "1"},
                                                                      {"added-files-size", "1391"},
                                                                      {"added-position-delete-files", "1"},
                                                                      {"added-position-deletes", "1"},
                                                                      {"changed-partition-count", "1"},
                                                                      {"operation", "overwrite"},
                                                                      {"spark.app.id", "local-1713951981838"},
                                                                      {"total-data-files", "6"},
                                                                      {"total-delete-files", "1"},
                                                                      {"total-equality-deletes", "0"},
                                                                      {"total-files-size", "26597"},
                                                                      {"total-position-deletes", "1"},
                                                                      {"total-records", "10000"}};

std::vector<SnapshotLog> expected_snapshot_log = {
    {1713951992417, 1638951453256129678}, {1713951995410, 5231658854638766100}, {1713951998102, 7558608030923099867}};

std::vector<MetadataLog> expected_metadata_log = {
    {1713951992417, "s3://warehouse/gperov/test/metadata/00000-800cc6aa-5051-47d5-9579-46aafcba1de6.metadata.json"},
    {1713951995410, "s3://warehouse/gperov/test/metadata/00001-6d216ef0-8d58-4f27-a1d9-1cb22c1f3415.metadata.json"},
    {1713951995685, "s3://warehouse/gperov/test/metadata/00002-37c508a5-8a06-4823-845e-889dff066f72.metadata.json"}};

std::vector<std::shared_ptr<PartitionSpec>> expected_partition_specs = {
    std::make_shared<PartitionSpec>(PartitionSpec{0, {}})};

std::vector<std::shared_ptr<SortOrder>> expected_sort_orders{std::make_shared<SortOrder>(SortOrder{0, {}})};

// std::map<std::string, SnapshotRef> expected_refs = {
//     {"main", SnapshotRef{.snapshot_id = 765518724043979080, .type = "branch"}}};

void Check(const TableMetadataV2& metadata) {
  EXPECT_EQ(metadata.location, "s3://warehouse/gperov/test");
  EXPECT_EQ(metadata.table_uuid, "4412d001-c6df-4adb-8854-d3b9e762440c");
  EXPECT_EQ(metadata.last_sequence_number, 3);
  EXPECT_EQ(metadata.last_updated_ms, 1713951998102);
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
  EXPECT_EQ(metadata.partition_specs.size(), expected_partition_specs.size());
  for (size_t i = 0; i < expected_partition_specs.size(); ++i) {
    EXPECT_EQ(*metadata.partition_specs[i], *expected_partition_specs[i]);
  }
  EXPECT_EQ(metadata.default_spec_id, 0);
  EXPECT_EQ(metadata.last_partition_id, 999);
  EXPECT_EQ(metadata.properties, expected_properties);
  EXPECT_EQ(metadata.current_snapshot_id, 7558608030923099867);
  EXPECT_TRUE(!metadata.snapshots.empty());
  const auto& snapshots = metadata.snapshots;
  EXPECT_EQ(snapshots.size(), 3);
  EXPECT_EQ(snapshots[1]->snapshot_id, 5231658854638766100);
  EXPECT_EQ(snapshots[1]->parent_snapshot_id.has_value(), true);
  EXPECT_EQ(snapshots[1]->sequence_number, 2);
  EXPECT_EQ(snapshots[1]->timestamp_ms, 1713951995410);
  EXPECT_EQ(snapshots[1]->summary, expected_summary_for_snapshot_0);
  EXPECT_EQ(snapshots[2]->schema_id, 0);
  EXPECT_EQ(snapshots[2]->snapshot_id, 7558608030923099867);
  EXPECT_EQ(snapshots[2]->parent_snapshot_id, 5231658854638766100);
  EXPECT_EQ(snapshots[2]->sequence_number, 3);
  EXPECT_EQ(snapshots[2]->timestamp_ms, 1713951998102);
  EXPECT_EQ(snapshots[2]->summary, expected_summary_for_snapshot_1);
  EXPECT_EQ(snapshots[2]->schema_id, 0);
  EXPECT_EQ(metadata.snapshot_log, expected_snapshot_log);
  EXPECT_EQ(metadata.metadata_log, expected_metadata_log);
  EXPECT_EQ(metadata.sort_orders.size(), expected_sort_orders.size());
  for (size_t i = 0; i < expected_sort_orders.size(); ++i) {
    EXPECT_EQ(*metadata.sort_orders[i], *expected_sort_orders[i]);
  }
  EXPECT_EQ(metadata.default_sort_order_id, 0);
  // EXPECT_EQ(metadata.refs, expected_refs);
}

}  // namespace

TEST(Metadata, ReadSanityCheck) {
  std::ifstream input("metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json");

  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  auto metadata = ice_tea::ReadTableMetadataV2(data);
  ASSERT_TRUE(!!metadata);
  Check(*metadata);
}

TEST(Metadata, ReadWriteRead) {
  std::ifstream input("metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json");

  auto metadata = ice_tea::ReadTableMetadataV2(input);
  ASSERT_TRUE(!!metadata);
  Check(*metadata);

  std::string serialized = ice_tea::WriteTableMetadataV2(*metadata, true);
  metadata = ice_tea::ReadTableMetadataV2(serialized);
  ASSERT_TRUE(!!metadata);
  Check(*metadata);
}

TEST(Metadata, ReadBrokenFiles) {
  std::ifstream input_empty("metadata/empty.metadata.json");
  EXPECT_FALSE(ice_tea::ReadTableMetadataV2(input_empty));

  std::ifstream input_broken1("metadata/broken1.metadata.json");
  EXPECT_FALSE(ice_tea::ReadTableMetadataV2(input_broken1));

  std::ifstream input_broken2("metadata/broken2.metadata.json");
  EXPECT_FALSE(ice_tea::ReadTableMetadataV2(input_broken2));

  std::ifstream input_ok("metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json");
  EXPECT_TRUE(ice_tea::ReadTableMetadataV2(input_ok));
}

TEST(Metadata, WithPartitionSpecs) {
  std::ifstream input("tables/partitioned_table/metadata/00001-3ac0dc8d-0a8e-44c2-b786-fff45a265023.metadata.json");

  auto metadata = ice_tea::ReadTableMetadataV2(input);
  ASSERT_TRUE(metadata);
  ASSERT_EQ(metadata->partition_specs.size(), 1);
  const auto& partition_spec = metadata->partition_specs[0];

  EXPECT_EQ(partition_spec->spec_id, 0);

  const auto& fields = partition_spec->fields;
  ASSERT_EQ(fields.size(), 2);

  const auto& field_0 = fields[0];
  EXPECT_EQ(field_0.field_id, 1000);
  EXPECT_EQ(field_0.name, "c1");
  EXPECT_EQ(field_0.source_id, 1);
  EXPECT_EQ(field_0.transform, "identity");

  const auto& field_1 = fields[1];
  EXPECT_EQ(field_1.field_id, 1001);
  EXPECT_EQ(field_1.name, "c2");
  EXPECT_EQ(field_1.source_id, 2);
  EXPECT_EQ(field_1.transform, "identity");
}

class MetadataYearPartitioningTest : public ::testing::Test {
 public:
  void Run(const std::string& metadata_path) {
    std::ifstream input(metadata_path);

    auto metadata = ice_tea::ReadTableMetadataV2(input);
    ASSERT_TRUE(metadata);
    ASSERT_EQ(metadata->partition_specs.size(), 1);
    const auto& partition_spec = metadata->partition_specs[0];

    EXPECT_EQ(partition_spec->spec_id, 0);

    const auto& fields = partition_spec->fields;
    ASSERT_EQ(fields.size(), 1);

    const auto& field = fields[0];
    EXPECT_EQ(field.name, "c2_year");
    EXPECT_EQ(field.transform, "year");
  }
};

TEST_F(MetadataYearPartitioningTest, Date) {
  Run("tables/year_date_partitioning/metadata/00002-b30c996e-fb0e-4ebc-a987-3536ceb792ea.metadata.json");
}

TEST_F(MetadataYearPartitioningTest, Timestamp) {
  Run("tables/year_timestamp_partitioning/metadata/00002-ac56aa65-6214-44b3-bb0f-86728eb58d8b.metadata.json");
}

TEST_F(MetadataYearPartitioningTest, Timestamptz) {
  Run("tables/year_timestamptz_partitioning/metadata/00002-d52e2c04-065b-4d14-98bb-ec47abcd1597.metadata.json");
}

TEST(Metadata, WithPartitionSpecsManyColumns) {
  std::ifstream input("tables/identity_partitioning/metadata/00002-30bff4d8-0c4f-46a9-8e7a-ebea458dbb1d.metadata.json");

  auto metadata = ice_tea::ReadTableMetadataV2(input);
  ASSERT_TRUE(metadata);
  ASSERT_EQ(metadata->partition_specs.size(), 1);
  const auto& partition_spec = metadata->partition_specs[0];

  EXPECT_EQ(partition_spec->spec_id, 0);

  const auto& fields = partition_spec->fields;
  ASSERT_EQ(fields.size(), 13);

  std::vector<std::string> column_names = {
      "col_bool", "col_int",       "col_long",        "col_float",  "col_double", "col_decimal",  "col_date",
      "col_time", "col_timestamp", "col_timestamptz", "col_string", "col_uuid",   "col_varbinary"};

  for (size_t i = 0; i < 13; ++i) {
    const auto& field = fields[i];
    EXPECT_EQ(field.name, column_names[i]);
    EXPECT_EQ(field.field_id, 1000 + i);
    EXPECT_EQ(field.source_id, i + 1);
    EXPECT_EQ(field.transform, "identity");
  }
}

TEST(Metadata, WithBucketPartitioning) {
  std::ifstream input("tables/bucket_partitioning/metadata/00002-53948f10-cced-409f-8dd9-6dea096895e8.metadata.json");

  auto metadata = ice_tea::ReadTableMetadataV2(input);
  ASSERT_TRUE(metadata);
  ASSERT_EQ(metadata->partition_specs.size(), 1);
  const auto& partition_spec = metadata->partition_specs[0];

  EXPECT_EQ(partition_spec->spec_id, 0);

  const auto& fields = partition_spec->fields;
  ASSERT_EQ(fields.size(), 10);

  std::vector<std::string> column_names = {"col_int",  "col_long",      "col_decimal",     "col_date",
                                           "col_time", "col_timestamp", "col_timestamptz", "col_string",
                                           "col_uuid", "col_varbinary"};

  std::vector<int> buckets = {2, 3, 4, 5, 6, 100, 123, 42, 55, 812};

  for (size_t i = 0; i < 10; ++i) {
    const auto& field = fields[i];
    EXPECT_EQ(field.name, column_names[i] + "_bucket");
    EXPECT_EQ(field.field_id, 1000 + i);
    EXPECT_EQ(field.source_id, i + 1);
    EXPECT_EQ(field.transform, "bucket[" + std::to_string(buckets[i]) + "]");
  }
}

}  // namespace iceberg
