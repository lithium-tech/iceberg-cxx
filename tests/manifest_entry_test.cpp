#include "iceberg/manifest_entry.h"

#include <Compiler.hh>
#include <NodeImpl.hh>
#include <Schema.hh>
#include <Types.hh>
#include <ValidSchema.hh>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <variant>
#include <vector>

#include "avro/Node.hh"
#include "gtest/gtest.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"

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

  Manifest manifest = ice_tea::ReadManifestEntries(data);
  Check(manifest.entries);
}

TEST(ManifestEntryTest, ReadWriteRead) {
  std::ifstream input("metadata/7e6e13cb-31fd-4de7-8811-02ce7cec44a9-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  Manifest manifest = ice_tea::ReadManifestEntries(data);
  auto metadata = manifest.metadata;
  Check(manifest.entries);

  std::string serialized = ice_tea::WriteManifestEntries(manifest);
  manifest = ice_tea::ReadManifestEntries(serialized);
  Check(manifest.entries);

  EXPECT_EQ(metadata, manifest.metadata);
  EXPECT_TRUE(metadata.contains("schema"));
  EXPECT_TRUE(metadata.contains("schema-id"));
  EXPECT_TRUE(metadata.contains("format-version"));
  EXPECT_TRUE(metadata.contains("partition-spec-id"));
  EXPECT_TRUE(metadata.contains("partition-spec"));
  EXPECT_TRUE(metadata.contains("content"));
}

TEST(ManifestEntryTest, Test2) {
  std::ifstream input("metadata/41f34bc8-eedf-4573-96b0-10c04e7c84c4-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  Manifest manifest = ice_tea::ReadManifestEntries(data);
  const auto& entries = manifest.entries;
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

TEST(ManifestEntryTest, FillSplitOffsets) {
  std::ifstream input("metadata/02ce7cec-31fd-4de7-8811-02ce7cec44a9-m0.avro");

  std::vector<ManifestEntry> entries = ice_tea::ReadManifestEntries(input).entries;

  // Clear offsets
  for (auto& entry : entries) {
    entry.data_file.split_offsets.clear();
  }
  std::shared_ptr<arrow::fs::FileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ice_tea::FillManifestSplitOffsets(entries, fs);

  std::ifstream input_copy("metadata/02ce7cec-31fd-4de7-8811-02ce7cec44a9-m0.avro");
  std::stringstream ss_copy;
  ss_copy << input_copy.rdbuf();
  std::string data_copy = ss_copy.str();

  Manifest entries_original = ice_tea::ReadManifestEntries(data_copy);

  for (size_t i = 0; i < entries.size(); ++i) {
    EXPECT_EQ(entries[i].data_file.split_offsets, entries_original.entries[i].data_file.split_offsets);
  }
}

TEST(ManifestEntryTest, FillSplitOffsets2) {
  std::ifstream input("metadata/02ce7cec-31fd-4de7-8811-02ce7cec44a9-m0.avro");
  std::vector<ManifestEntry> entries = ice_tea::ReadManifestEntries(input).entries;

  // Clear offsets
  for (auto& entry : entries) {
    entry.data_file.split_offsets.clear();
  }

  std::shared_ptr<arrow::fs::FileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();

  std::vector<std::shared_ptr<parquet::FileMetaData>> metadata;
  for (auto& entry : entries) {
    uint64_t file_size;
    auto parquet_meta = ParquetMetadata(fs, entry.data_file.file_path, file_size);
    metadata.push_back(std::move(parquet_meta));
  }

  ice_tea::FillManifestSplitOffsets(entries, metadata);

  std::ifstream input_copy("metadata/02ce7cec-31fd-4de7-8811-02ce7cec44a9-m0.avro");
  std::vector<ManifestEntry> entries_original = ice_tea::ReadManifestEntries(input_copy).entries;

  for (size_t i = 0; i < entries.size(); ++i) {
    EXPECT_EQ(entries[i].data_file.split_offsets, entries_original[i].data_file.split_offsets);
  }
}

TEST(ManifestEntryTest, ReadBrokenFiles) {
  std::ifstream input_empty("metadata/empty.avro");
  EXPECT_THROW(ice_tea::ReadManifestEntries(input_empty), std::runtime_error);

  std::ifstream input_broken1("metadata/broken1.avro");
  EXPECT_THROW(ice_tea::ReadManifestEntries(input_broken1), std::runtime_error);

  std::ifstream input_broken2("metadata/broken2.avro");
  EXPECT_THROW(ice_tea::ReadManifestEntries(input_broken2), std::runtime_error);

  std::ifstream input_ok("metadata/02ce7cec-31fd-4de7-8811-02ce7cec44a9-m0.avro");
  EXPECT_NO_THROW(ice_tea::ReadManifestEntries(input_ok));
}

bool IsLess(const ContentFile::PartitionKey& lhs, const ContentFile::PartitionKey& rhs) {
  if (lhs.name != rhs.name) {
    return lhs.name < rhs.name;
  }
  return lhs.value < rhs.value;
}

bool IsLessVec(const ContentFile::PartitionTuple& lhs, const ContentFile::PartitionTuple& rhs) {
  if (lhs.fields.size() != rhs.fields.size()) {
    return lhs.fields.size() < rhs.fields.size();
  }
  for (size_t i = 0; i < lhs.fields.size(); ++i) {
    const auto& lhs_field = lhs.fields[i];
    const auto& rhs_field = rhs.fields[i];
    if (IsLess(lhs_field, rhs_field)) {
      return true;
    }
    if (IsLess(rhs_field, lhs_field)) {
      return false;
    }
  }
  return false;
}

void ComparePartitionTuples(const DataFile::PartitionTuple& lhs, const DataFile::PartitionTuple& rhs,
                            const std::string& hint) {
  ASSERT_EQ(lhs.fields.size(), rhs.fields.size());

  for (size_t i = 0; i < lhs.fields.size(); ++i) {
    const auto& lhs_field = lhs.fields[i];
    const auto& rhs_field = rhs.fields[i];
    EXPECT_EQ(lhs_field.name, rhs_field.name) << "index: " << i << ", hint: " << hint;
    EXPECT_EQ(lhs_field.value.index(), rhs_field.value.index()) << "index: " << i << ", hint: " << hint;
    if (lhs_field.value.index() == rhs_field.value.index()) {
      if (std::holds_alternative<DataFile::PartitionKey::Fixed>(lhs_field.value)) {
        EXPECT_EQ(std::get<DataFile::PartitionKey::Fixed>(lhs_field.value).bytes,
                  std::get<DataFile::PartitionKey::Fixed>(rhs_field.value).bytes)
            << "index: " << i << ", hint: " << hint;
      } else {
        EXPECT_EQ(lhs_field.value, rhs_field.value) << "index: " << i << ", hint: " << hint;
      }
    }
    bool is_lhs_timestamp = lhs_field.type && (lhs_field.type->TypeId() == TypeID::kTimestamp ||
                                               lhs_field.type->TypeId() == TypeID::kTimestamptz);
    bool is_rhs_timestamp = rhs_field.type && (rhs_field.type->TypeId() == TypeID::kTimestamp ||
                                               rhs_field.type->TypeId() == TypeID::kTimestamptz);
    if (is_lhs_timestamp && is_rhs_timestamp) {
      continue;
    }
    const std::string lhs_string = lhs_field.type ? lhs_field.type->ToString() : "null";
    const std::string rhs_string = rhs_field.type ? rhs_field.type->ToString() : "null";
    EXPECT_EQ(lhs_string, rhs_string) << "index: " << i << ", hint: " << hint;
  }
}

TEST(ManifestEntryTest, TestPartitioned) {
  std::ifstream input("tables/partitioned_table/metadata/8968fbb0-57cf-40b0-a725-7d147f07b4b8-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ice_tea::PartitionKeyField> fields = {
      ice_tea::PartitionKeyField("c1", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("c2", std::make_shared<types::PrimitiveType>(TypeID::kDate))};

  Manifest manifest = ice_tea::ReadManifestEntries(data);
  ASSERT_EQ(manifest.entries.size(), 6);
  std::vector<DataFile::PartitionTuple> infos;
  for (size_t i = 0; i < 6; ++i) {
    infos.emplace_back(manifest.entries[i].data_file.partition_tuple);
  }

  std::shared_ptr<iceberg::Schema> schema;

  using PI = DataFile::PartitionTuple;
  using PF = DataFile::PartitionKey;

  auto int_type = std::make_shared<types::PrimitiveType>(TypeID::kInt);
  auto date_type = std::make_shared<types::PrimitiveType>(TypeID::kDate);

  std::vector<PI> expected_infos;
  expected_infos.push_back(PI{.fields = {PF("c1", 1, int_type), PF("c2", 20149, date_type)}});
  expected_infos.push_back(PI{.fields = {PF("c1", 1, int_type), PF("c2", 20150, date_type)}});
  expected_infos.push_back(PI{.fields = {PF("c1", 1, int_type), PF("c2", 20151, date_type)}});
  expected_infos.push_back(PI{.fields = {PF("c1", 1, int_type), PF("c2", 20152, date_type)}});
  expected_infos.push_back(PI{.fields = {PF("c1", 2, int_type), PF("c2", 20150, date_type)}});
  expected_infos.push_back(PI{.fields = {PF("c1", 2, int_type), PF("c2", 20151, date_type)}});

  std::sort(infos.begin(), infos.end(), IsLessVec);
  std::sort(expected_infos.begin(), expected_infos.end(), IsLessVec);
  for (size_t i = 0; i < infos.size(); ++i) {
    ComparePartitionTuples(infos[i], expected_infos[i], std::to_string(__LINE__) + "." + std::to_string(i));
  }

  data = ice_tea::WriteManifestEntries(manifest, fields);
  std::vector<DataFile::PartitionTuple> new_infos;
  manifest = ice_tea::ReadManifestEntries(data);
  for (size_t i = 0; i < 6; ++i) {
    new_infos.emplace_back(manifest.entries[i].data_file.partition_tuple);
  }

  std::sort(new_infos.begin(), new_infos.end(), IsLessVec);
  for (size_t i = 0; i < infos.size(); ++i) {
    ComparePartitionTuples(new_infos[i], expected_infos[i], std::to_string(__LINE__) + "." + std::to_string(i));
  }
}

TEST(ManifestEntryTest, TestPartitionedWithNull) {
  std::ifstream input("tables/partitioning_with_null_value/metadata/5fdf7a4a-b0c5-465e-84a3-bed9a79cca3b-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ice_tea::PartitionKeyField> fields = {
      ice_tea::PartitionKeyField("c1", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("c2", std::make_shared<types::PrimitiveType>(TypeID::kDate))};

  Manifest manifest = ice_tea::ReadManifestEntries(data);
  ASSERT_EQ(manifest.entries.size(), 4);
  std::vector<DataFile::PartitionTuple> infos;
  for (size_t i = 0; i < 4; ++i) {
    infos.emplace_back(manifest.entries[i].data_file.partition_tuple);
  }

  std::shared_ptr<iceberg::Schema> schema;

  using PI = DataFile::PartitionTuple;
  using PF = DataFile::PartitionKey;

  auto int_type = std::make_shared<types::PrimitiveType>(TypeID::kInt);
  auto date_type = std::make_shared<types::PrimitiveType>(TypeID::kDate);

  std::vector<PI> expected_infos;
  expected_infos.push_back(PI{.fields = {PF("c1", 2, int_type), PF("c2", 20150, date_type)}});
  expected_infos.push_back(PI{.fields = {PF("c1"), PF("c2", 20150, date_type)}});
  expected_infos.push_back(PI{.fields = {PF("c1", 1, int_type), PF("c2")}});
  expected_infos.push_back(PI{.fields = {PF("c1"), PF("c2")}});

  std::sort(infos.begin(), infos.end(), IsLessVec);
  std::sort(expected_infos.begin(), expected_infos.end(), IsLessVec);

  for (size_t i = 0; i < infos.size(); ++i) {
    ComparePartitionTuples(infos[i], expected_infos[i], std::to_string(__LINE__) + "." + std::to_string(i));
  }

  data = ice_tea::WriteManifestEntries(manifest, fields);
  std::vector<DataFile::PartitionTuple> new_infos;
  manifest = ice_tea::ReadManifestEntries(data);
  for (size_t i = 0; i < 4; ++i) {
    new_infos.emplace_back(manifest.entries[i].data_file.partition_tuple);
  }

  std::sort(new_infos.begin(), new_infos.end(), IsLessVec);
  for (size_t i = 0; i < infos.size(); ++i) {
    ComparePartitionTuples(new_infos[i], expected_infos[i], std::to_string(__LINE__) + "." + std::to_string(i));
  }
}

class ManifestYearPartitioningTest : public ::testing::Test {
 public:
  void Run(const std::string& metadata_path) {
    std::ifstream input(metadata_path);
    std::stringstream ss;
    ss << input.rdbuf();
    std::string data = ss.str();

    std::vector<ice_tea::PartitionKeyField> fields = {
        ice_tea::PartitionKeyField("c2_year", std::make_shared<types::PrimitiveType>(TypeID::kInt))};

    Manifest manifest = ice_tea::ReadManifestEntries(data);
    std::sort(manifest.entries.begin(), manifest.entries.end(), [&](const auto& lhs, const auto& rhs) {
      return lhs.data_file.record_count < rhs.data_file.record_count;
    });

    ASSERT_EQ(manifest.entries.size(), 2);

    ASSERT_EQ(manifest.entries[1].data_file.partition_tuple.fields.size(), 1);
    auto field = manifest.entries[0].data_file.partition_tuple.fields[0];

    EXPECT_EQ(field.name, "c2_year");
    ASSERT_TRUE(std::holds_alternative<int>(field.value));
    EXPECT_EQ(std::get<int>(field.value), 54);

    data = ice_tea::WriteManifestEntries(manifest, fields);

    std::vector<DataFile::PartitionTuple> new_infos;
    manifest = ice_tea::ReadManifestEntries(data);
    std::sort(manifest.entries.begin(), manifest.entries.end(), [&](const auto& lhs, const auto& rhs) {
      return lhs.data_file.record_count < rhs.data_file.record_count;
    });
    field = manifest.entries[0].data_file.partition_tuple.fields[0];
    EXPECT_EQ(field.name, "c2_year");
    ASSERT_TRUE(std::holds_alternative<int>(field.value));
    EXPECT_EQ(std::get<int>(field.value), 54);
  }
};

TEST_F(ManifestYearPartitioningTest, Date) {
  Run("tables/year_date_partitioning/metadata/48510c7c-7855-4273-9170-5de3130502f6-m0.avro");
}

TEST_F(ManifestYearPartitioningTest, Timestamp) {
  Run("tables/year_timestamp_partitioning/metadata/f6bc700c-2909-411e-b32c-6c3c74a447ae-m0.avro");
}

TEST_F(ManifestYearPartitioningTest, Timestamptz) {
  Run("tables/year_timestamptz_partitioning/metadata/26969caf-f1a0-41ee-8ec9-8b442ed19ecc-m0.avro");
}

TEST(ManifestEntryTest, TestPartitionedManyTypes) {
  std::ifstream input("tables/identity_partitioning/metadata/bfd02a62-ed9d-4ab0-83a0-992b76310fe4-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ice_tea::PartitionKeyField> fields = {
    ice_tea::PartitionKeyField("col_bool", std::make_shared<types::PrimitiveType>(TypeID::kBoolean)),
    ice_tea::PartitionKeyField("col_int", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
    ice_tea::PartitionKeyField("col_long", std::make_shared<types::PrimitiveType>(TypeID::kLong)),
    ice_tea::PartitionKeyField("col_float", std::make_shared<types::PrimitiveType>(TypeID::kFloat)),
    ice_tea::PartitionKeyField("col_double", std::make_shared<types::PrimitiveType>(TypeID::kDouble)),
    ice_tea::PartitionKeyField("col_decimal", std::make_shared<types::DecimalType>(9, 2)),
    ice_tea::PartitionKeyField("col_date", std::make_shared<types::PrimitiveType>(TypeID::kDate)),
    ice_tea::PartitionKeyField("col_time", std::make_shared<types::PrimitiveType>(TypeID::kTime)),
    ice_tea::PartitionKeyField("col_timestamp", std::make_shared<types::PrimitiveType>(TypeID::kTimestamp)),
    ice_tea::PartitionKeyField("col_timestamptz", std::make_shared<types::PrimitiveType>(TypeID::kTimestamp)),
    ice_tea::PartitionKeyField("col_string", std::make_shared<types::PrimitiveType>(TypeID::kString)),

#if 0
    ice_tea::PartitionKeyField("col_uuid", std::make_shared<types::PrimitiveType>(TypeID::kUuid)),
#else
    ice_tea::PartitionKeyField("col_uuid", std::make_shared<types::FixedType>(16)),
#endif

    ice_tea::PartitionKeyField("col_varbinary", std::make_shared<types::PrimitiveType>(TypeID::kBinary))
  };

  using PF = DataFile::PartitionKey;

  std::vector<std::string> column_names = {
      "col_bool", "col_int",       "col_long",        "col_float",  "col_double", "col_decimal",  "col_date",
      "col_time", "col_timestamp", "col_timestamptz", "col_string", "col_uuid",   "col_varbinary"};
  using ValueHolder = ContentFile::PartitionKey::AvroValueHolder;
  DataFile::PartitionKey::Fixed expected_uuid{
      .bytes = {0x12, 0x15, 0x1f, 0xd2, 0x75, 0x86, 0x11, 0xe9, 0x8f, 0x9e, 0x2a, 0x86, 0xe4, 0x08, 0x5a, 0x59}};
  DataFile::PartitionKey::Fixed expected_decimal{.bytes = {0, 0, 0x01, 0x2C}};
  std::vector<uint8_t> expected_col_fixed_value{'s', 'o', 'm', 'e', '-', 'v', 'a', 'r', 'b', 'i', 'n', 'a', 'r', 'y'};

  DataFile::PartitionTuple expected_partition_info{
      .fields = {
          PF("col_bool", false, std::make_shared<types::PrimitiveType>(TypeID::kBoolean)),
          PF("col_int", 1, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
          PF("col_long", int64_t(2), std::make_shared<types::PrimitiveType>(TypeID::kLong)),
          PF("col_float", float(2.25), std::make_shared<types::PrimitiveType>(TypeID::kFloat)),
          PF("col_double", double(2.375), std::make_shared<types::PrimitiveType>(TypeID::kDouble)),
          PF("col_decimal", expected_decimal, std::make_shared<types::DecimalType>(9, 2)),
          PF("col_date", 20150, std::make_shared<types::PrimitiveType>(TypeID::kDate)),
          PF("col_time", int64_t(61200000000), std::make_shared<types::PrimitiveType>(TypeID::kTime)),
          PF("col_timestamp", 1740960000000000, std::make_shared<types::PrimitiveType>(TypeID::kTimestamp)),
          PF("col_timestamptz", 1740960000000000, std::make_shared<types::PrimitiveType>(TypeID::kTimestamptz)),
          PF("col_string", "some-string", std::make_shared<types::PrimitiveType>(TypeID::kString)),
          PF("col_uuid", expected_uuid, std::make_shared<types::FixedType>(16)),
          PF("col_varbinary", expected_col_fixed_value, std::make_shared<types::PrimitiveType>(TypeID::kBinary)),
      }};

  std::sort(expected_partition_info.fields.begin(), expected_partition_info.fields.end(), IsLess);
  Manifest manifest = ice_tea::ReadManifestEntries(data);
  ASSERT_EQ(manifest.entries.size(), 1);
  auto info = manifest.entries[0].data_file.partition_tuple;
  ComparePartitionTuples(expected_partition_info, info, std::to_string(__LINE__));

  data = ice_tea::WriteManifestEntries(manifest, fields);
  std::vector<DataFile::PartitionTuple> new_infos;
  manifest = ice_tea::ReadManifestEntries(data);
  info = manifest.entries[0].data_file.partition_tuple;
  ComparePartitionTuples(expected_partition_info, info, std::to_string(__LINE__));
}

TEST(ManifestEntryTest, DecimalPartitioning) {
  std::ifstream input("tables/decimal_partitioning/metadata/20d87eae-b5a0-4f44-b543-9abfbd448b70-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ice_tea::PartitionKeyField> fields = {
      ice_tea::PartitionKeyField("col_decimal_9_2", std::make_shared<types::DecimalType>(9, 2)),
      ice_tea::PartitionKeyField("col_decimal_18_2", std::make_shared<types::DecimalType>(18, 2)),
      ice_tea::PartitionKeyField("col_decimal_19_2", std::make_shared<types::DecimalType>(19, 2)),
      ice_tea::PartitionKeyField("col_decimal_38_2", std::make_shared<types::DecimalType>(38, 2))};

  using PF = DataFile::PartitionKey;

  DataFile::PartitionKey::Fixed expected_decimal_9{.bytes = {0, 0, 0x01, 0x2C}};
  DataFile::PartitionKey::Fixed expected_decimal_18{.bytes = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0x0C}};
  DataFile::PartitionKey::Fixed expected_decimal_19{.bytes = std::vector<uint8_t>(9, 0)};
  DataFile::PartitionKey::Fixed expected_decimal_38{.bytes = std::vector<uint8_t>(16, 0)};

  DataFile::PartitionTuple expected_partition_info{
      .fields = {PF("col_decimal_9_2", expected_decimal_9, std::make_shared<types::DecimalType>(9, 2)),
                 PF("col_decimal_18_2", expected_decimal_18, std::make_shared<types::DecimalType>(18, 2)),
                 PF("col_decimal_19_2", expected_decimal_19, std::make_shared<types::DecimalType>(19, 2)),
                 PF("col_decimal_38_2", expected_decimal_38, std::make_shared<types::DecimalType>(38, 2))}};

  Manifest manifest = ice_tea::ReadManifestEntries(data);
  ASSERT_EQ(manifest.entries.size(), 1);
  auto info = manifest.entries[0].data_file.partition_tuple;

  std::sort(expected_partition_info.fields.begin(), expected_partition_info.fields.end(), IsLess);
  ComparePartitionTuples(expected_partition_info, info, std::to_string(__LINE__));

  data = ice_tea::WriteManifestEntries(manifest, fields);
  std::vector<DataFile::PartitionTuple> new_infos;
  manifest = ice_tea::ReadManifestEntries(data);
  info = manifest.entries[0].data_file.partition_tuple;
  ComparePartitionTuples(expected_partition_info, info, std::to_string(__LINE__));
}

TEST(ManifestEntryTest, TestBucketPartitioning) {
  std::ifstream input("tables/bucket_partitioning/metadata/0ffce867-cc54-49f8-ac74-c89fc147015d-m0.avro");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  std::vector<ice_tea::PartitionKeyField> fields = {
      ice_tea::PartitionKeyField("col_int_bucket", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("col_long_bucket", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("col_decimal_bucket", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("col_date_bucket", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("col_time_bucket", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("col_timestamp_bucket", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("col_timestamptz_bucket", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("col_string_bucket", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("col_uuid_bucket", std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      ice_tea::PartitionKeyField("col_varbinary_bucket", std::make_shared<types::PrimitiveType>(TypeID::kInt))};

  using PF = DataFile::PartitionKey;

  DataFile::PartitionTuple expected_partition_info{
      .fields = {
          PF("col_int_bucket", 0, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
          PF("col_long_bucket", 0, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
          PF("col_decimal_bucket", 3, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
          PF("col_date_bucket", 2, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
          PF("col_time_bucket", 2, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
          PF("col_timestamp_bucket", 99, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
          PF("col_timestamptz_bucket", 75, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
          PF("col_string_bucket", 13, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
          PF("col_uuid_bucket", 5, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
          PF("col_varbinary_bucket", 724, std::make_shared<types::PrimitiveType>(TypeID::kInt)),
      }};

  Manifest manifest = ice_tea::ReadManifestEntries(data);
  ASSERT_EQ(manifest.entries.size(), 1);
  auto info = manifest.entries[0].data_file.partition_tuple;
  std::sort(expected_partition_info.fields.begin(), expected_partition_info.fields.end(), IsLess);
  ComparePartitionTuples(expected_partition_info, info, std::to_string(__LINE__));

  data = ice_tea::WriteManifestEntries(manifest, fields);
  std::vector<DataFile::PartitionTuple> new_infos;
  manifest = ice_tea::ReadManifestEntries(data);
  info = manifest.entries[0].data_file.partition_tuple;
  ComparePartitionTuples(expected_partition_info, info, std::to_string(__LINE__));
}

}  // namespace iceberg
