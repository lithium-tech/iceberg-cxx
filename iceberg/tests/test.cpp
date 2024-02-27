#include "gtest/gtest.h"
#include "iceberg/src/metadata.h"

namespace iceberg {

TEST(Metadata, SanityCheck) {
  const std::string json = R"EOF(
{
  "format-version" : 2,
  "table-uuid" : "a319422b-6f8c-44d0-90ba-96242d9a1d7b",
  "location" : "./lineitem_iceberg",
  "last-sequence-number" : 2,
  "last-updated-ms" : 1676473694730,
  "last-column-id" : 16,
  "current-schema-id" : 0,
  "schemas" : [ {
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [ {
      "id" : 1,
      "name" : "l_orderkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 2,
      "name" : "l_partkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 3,
      "name" : "l_suppkey",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 4,
      "name" : "l_linenumber",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 5,
      "name" : "l_quantity",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 6,
      "name" : "l_extendedprice",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 7,
      "name" : "l_discount",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 8,
      "name" : "l_tax",
      "required" : false,
      "type" : "decimal(15, 2)"
    }, {
      "id" : 9,
      "name" : "l_returnflag",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 10,
      "name" : "l_linestatus",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 11,
      "name" : "l_shipdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 12,
      "name" : "l_commitdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 13,
      "name" : "l_receiptdate",
      "required" : false,
      "type" : "date"
    }, {
      "id" : 14,
      "name" : "l_shipinstruct",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 15,
      "name" : "l_shipmode",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 16,
      "name" : "l_comment",
      "required" : false,
      "type" : "string"
    } ]
  } ],
  "default-spec-id" : 0,
  "partition-specs" : [ {
    "spec-id" : 0,
    "fields" : [ ]
  } ],
  "last-partition-id" : 999,
  "default-sort-order-id" : 0,
  "sort-orders" : [ {
    "order-id" : 0,
    "fields" : [ ]
  } ],
  "properties" : {
    "owner" : "root",
    "write.update.mode" : "merge-on-read"
  },
  "current-snapshot-id" : 7635660646343998149,
  "refs" : {
    "main" : {
      "snapshot-id" : 7635660646343998149,
      "type" : "branch"
    }
  },
  "snapshots" : [ {
    "sequence-number" : 1,
    "snapshot-id" : 3776207205136740581,
    "timestamp-ms" : 1676473674504,
    "summary" : {
      "operation" : "append",
      "spark.app.id" : "local-1676472783435",
      "added-data-files" : "1",
      "added-records" : "60175",
      "added-files-size" : "1390176",
      "changed-partition-count" : "1",
      "total-records" : "60175",
      "total-files-size" : "1390176",
      "total-data-files" : "1",
      "total-delete-files" : "0",
      "total-position-deletes" : "0",
      "total-equality-deletes" : "0"
    },
    "manifest-list" : "lineitem_iceberg/metadata/snap-3776207205136740581-1-cf3d0be5-cf70-453d-ad8f-48fdc412e608.avro",
    "schema-id" : 0
  }, {
    "sequence-number" : 2,
    "snapshot-id" : 7635660646343998149,
    "parent-snapshot-id" : 3776207205136740581,
    "timestamp-ms" : 1676473694730,
    "summary" : {
      "operation" : "overwrite",
      "spark.app.id" : "local-1676472783435",
      "added-data-files" : "1",
      "deleted-data-files" : "1",
      "added-records" : "51793",
      "deleted-records" : "60175",
      "added-files-size" : "1208539",
      "removed-files-size" : "1390176",
      "changed-partition-count" : "1",
      "total-records" : "51793",
      "total-files-size" : "1208539",
      "total-data-files" : "1",
      "total-delete-files" : "0",
      "total-position-deletes" : "0",
      "total-equality-deletes" : "0"
    },
    "manifest-list" : "lineitem_iceberg/metadata/snap-7635660646343998149-1-10eaca8a-1e1c-421e-ad6d-b232e5ee23d3.avro",
    "schema-id" : 0
  } ],
  "snapshot-log" : [ {
    "timestamp-ms" : 1676473674504,
    "snapshot-id" : 3776207205136740581
  }, {
    "timestamp-ms" : 1676473694730,
    "snapshot-id" : 7635660646343998149
  } ],
  "metadata-log" : [ {
    "timestamp-ms" : 1676473674504,
    "metadata-file" : "lineitem_iceberg/metadata/v1.metadata.json"
  } ]
})EOF";

  const TableMetadataV2 metadata = MakeMetadata(json);
  EXPECT_EQ(metadata.location, "./lineitem_iceberg");
  EXPECT_EQ(metadata.table_uuid, "a319422b-6f8c-44d0-90ba-96242d9a1d7b");
  EXPECT_EQ(metadata.last_sequence_number, 2);
  EXPECT_EQ(metadata.last_updated_ms, 1676473694730);
  EXPECT_EQ(metadata.last_column_id, 16);
  EXPECT_EQ(metadata.schemas.size(), 1);
  EXPECT_EQ(metadata.schemas[0].GetSchemaId(), 0);
  EXPECT_EQ(metadata.schemas[0].GetFields().size(), 16);
  auto& field = metadata.schemas[0].GetFields()[7];
  EXPECT_EQ(field.id, 8);
  EXPECT_EQ(field.is_required, false);
  EXPECT_EQ(field.name, "l_tax");
  ASSERT_EQ(field.type->IsDecimal(), true);
  auto decimal_type =
      std::static_pointer_cast<const DecimalDataType>(field.type);
  EXPECT_EQ(decimal_type->Precision(), 15);
  EXPECT_EQ(decimal_type->Scale(), 2);
  EXPECT_EQ(metadata.current_schema_id, 0);
  EXPECT_EQ(metadata.default_spec_id, 0);
  EXPECT_EQ(metadata.last_partition_id, 999);
  std::map<std::string, std::string> expected_properties = {
      {"owner", "root"}, {"write.update.mode", "merge-on-read"}};
  EXPECT_EQ(metadata.properties, expected_properties);
  EXPECT_EQ(metadata.current_snapshot_id, 7635660646343998149);
  EXPECT_EQ(metadata.snapshots.has_value(), true);
  const auto& snapshots = metadata.snapshots.value();
  EXPECT_EQ(snapshots.size(), 2);
  EXPECT_EQ(snapshots[0].snapshot_id, 3776207205136740581);
  EXPECT_EQ(snapshots[0].parent_snapshot_id.has_value(), false);
  EXPECT_EQ(snapshots[0].sequence_number, 1);
  EXPECT_EQ(snapshots[0].timestamp_ms, 1676473674504);
  std::map<std::string, std::string> expected_summary_for_snapshot_0 = {
      {"operation", "append"},         {"spark.app.id", "local-1676472783435"},
      {"added-data-files", "1"},       {"added-records", "60175"},
      {"added-files-size", "1390176"}, {"changed-partition-count", "1"},
      {"total-records", "60175"},      {"total-files-size", "1390176"},
      {"total-data-files", "1"},       {"total-delete-files", "0"},
      {"total-position-deletes", "0"}, {"total-equality-deletes", "0"}};
  EXPECT_EQ(snapshots[0].summary, expected_summary_for_snapshot_0);
  EXPECT_EQ(snapshots[0].schema_id, 0);
  EXPECT_EQ(snapshots[1].snapshot_id, 7635660646343998149);
  EXPECT_EQ(snapshots[1].parent_snapshot_id, 3776207205136740581);
  EXPECT_EQ(snapshots[1].sequence_number, 2);
  EXPECT_EQ(snapshots[1].timestamp_ms, 1676473694730);
  std::map<std::string, std::string> expected_summary_for_snapshot_1 = {
      {"operation", "overwrite"},       {"spark.app.id", "local-1676472783435"},
      {"added-data-files", "1"},        {"deleted-data-files", "1"},
      {"added-records", "51793"},       {"deleted-records", "60175"},
      {"added-files-size", "1208539"},  {"removed-files-size", "1390176"},
      {"changed-partition-count", "1"}, {"total-records", "51793"},
      {"total-files-size", "1208539"},  {"total-data-files", "1"},
      {"total-delete-files", "0"},      {"total-position-deletes", "0"},
      {"total-equality-deletes", "0"}};
  EXPECT_EQ(snapshots[1].summary, expected_summary_for_snapshot_1);
  EXPECT_EQ(snapshots[1].schema_id, 0);
  std::vector<std::pair<int64_t, int64_t>> expected_snapshot_log = {
      {1676473674504, 3776207205136740581},
      {1676473694730, 7635660646343998149}};
  EXPECT_EQ(metadata.snapshot_log, expected_snapshot_log);
  std::vector<std::pair<int64_t, std::string>> expected_metadata_log = {
      {1676473674504, "lineitem_iceberg/metadata/v1.metadata.json"}};
  EXPECT_EQ(metadata.metadata_log, expected_metadata_log);
  EXPECT_EQ(metadata.default_sort_order_id, 0);
}

}  // namespace iceberg
