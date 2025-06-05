#include <fstream>

#include "gtest/gtest.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "parquet/file_reader.h"

namespace {
std::shared_ptr<iceberg::TableMetadataV2> GetMetadata(const std::string& path) {
  std::ifstream input(path);

  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  auto metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  EXPECT_NE(metadata, nullptr);
  return metadata;
}

inline std::string LogicalTypeErrorMessage(std::shared_ptr<const iceberg::types::Type> iceberg_type,
                                           std::shared_ptr<const parquet::LogicalType> parquet_type) {
  return "Iceberg " + iceberg_type->ToString() + " column must be represented as parquet " + parquet_type->ToString() +
         " logical type\n";
}

inline std::string PhysicalTypeErrorMessage(std::shared_ptr<const iceberg::types::Type> iceberg_type,
                                            parquet::Type::type parquet_type) {
  return "Iceberg " + iceberg_type->ToString() + " column must be represented as parquet " +
         parquet::TypeToString(parquet_type) + " physical type\n";
}

constexpr char kDecimalsMetadata[] =
    "tables/types/decimals/metadata/00000-93fb1a3e-cf50-4083-af65-68d07b84c5ad.metadata.json";
constexpr char kDecimalsData[] =
    "tables/types/decimals/data/00007-7-7b64d752-fbdc-48ca-b1a2-b88f692557ff-0-00001.parquet";

constexpr char kAllSparkTypesMetadata[] =
    "tables/types/all_spark_types/metadata/00000-4f9d7c78-62f9-48c6-b869-f25172b45b32.metadata.json";
constexpr char kAllSparkTypesData[] =
    "tables/types/all_spark_types/data/00007-7-08e6240e-b5f7-48ad-b7f0-039dc52bc563-0-00001.parquet";

constexpr char kAllTrinoTypesMetadata[] =
    "tables/types/all_trino_types/metadata/00002-6105d5eb-ce39-470c-af78-50b744662ef2.metadata.json";
constexpr char kAllTrinoTypesData[] =
    "tables/types/all_trino_types/data/20250601_201751_00003_935ws-92e43a1f-75de-42b6-87bf-85642c280237.parquet";

}  // namespace

TEST(SchemaValidationTest, Decimal) {
  auto metadata = GetMetadata(kDecimalsMetadata);

  auto reader = parquet::ParquetFileReader::OpenFile(kDecimalsData);
  auto parquet_metadata = reader->metadata();
  EXPECT_NO_THROW(
      iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(), *parquet_metadata->schema()));
}

TEST(SchemaValidationTest, DecimalNoThrow) {
  auto metadata = GetMetadata(kDecimalsMetadata);

  auto reader = parquet::ParquetFileReader::OpenFile(kDecimalsData);
  auto parquet_metadata = reader->metadata();
  EXPECT_TRUE(iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(),
                                                                 *parquet_metadata->schema(), false));
}

TEST(SchemaValidationTest, AllSparkTypes) {
  auto metadata = GetMetadata(kAllSparkTypesMetadata);

  auto reader = parquet::ParquetFileReader::OpenFile(kAllSparkTypesData);
  auto parquet_metadata = reader->metadata();
  EXPECT_NO_THROW(
      iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(), *parquet_metadata->schema()));
}

TEST(SchemaValidationTest, AllTrinoTypes) {
  auto metadata = GetMetadata(kAllTrinoTypesMetadata);

  auto reader = parquet::ParquetFileReader::OpenFile(kAllTrinoTypesData);
  auto parquet_metadata = reader->metadata();
  EXPECT_NO_THROW(
      iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(), *parquet_metadata->schema()));
}

TEST(SchemaValidationTest, MappingWithFieldID1) {
  auto schema = GetMetadata(kAllSparkTypesMetadata)->GetCurrentSchema();
  std::vector<iceberg::types::NestedField> fake_columns(schema->Columns());
  std::swap(fake_columns[0].field_id, fake_columns[1].field_id);
  iceberg::Schema fake_schema(schema->SchemaId(), fake_columns);

  auto reader = parquet::ParquetFileReader::OpenFile(kAllSparkTypesData);
  auto parquet_metadata = reader->metadata();

  EXPECT_THROW(iceberg::IcebergToParquetSchemaValidator::Validate(fake_schema, *parquet_metadata->schema()),
               std::runtime_error);
}

TEST(SchemaValidationTest, MappingWithFieldID2) {
  auto schema = GetMetadata(kAllSparkTypesMetadata)->GetCurrentSchema();
  std::vector<iceberg::types::NestedField> fake_columns(schema->Columns());
  for (auto& col : fake_columns) {
    col.field_id = -1;
  }
  iceberg::Schema fake_schema(schema->SchemaId(), fake_columns);

  // note here is kAllTrinoTypesData, not kAllSparkTypesData
  auto reader = parquet::ParquetFileReader::OpenFile(kAllTrinoTypesData);
  auto parquet_metadata = reader->metadata();

  EXPECT_NO_THROW(iceberg::IcebergToParquetSchemaValidator::Validate(fake_schema, *parquet_metadata->schema()));
}

TEST(SchemaValidationTest, TimestampNeqTimestamptz) {
  auto schema = GetMetadata(kAllSparkTypesMetadata)->GetCurrentSchema();
  std::vector<iceberg::types::NestedField> fake_columns(schema->Columns());
  EXPECT_EQ(fake_columns[9].type->TypeId(), iceberg::TypeID::kTimestamptz);
  EXPECT_EQ(fake_columns[10].type->TypeId(), iceberg::TypeID::kTimestamp);
  std::swap(fake_columns[9].type, fake_columns[10].type);
  iceberg::Schema fake_schema(schema->SchemaId(), fake_columns);

  auto reader = parquet::ParquetFileReader::OpenFile(kAllSparkTypesData);
  auto parquet_metadata = reader->metadata();

  const std::string expected_substring1 =
      LogicalTypeErrorMessage(std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kTimestamp),
                              parquet::LogicalType::Timestamp(false, parquet::LogicalType::TimeUnit::MICROS));
  const std::string expected_substring2 =
      LogicalTypeErrorMessage(std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kTimestamptz),
                              parquet::LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::MICROS));

  try {
    iceberg::IcebergToParquetSchemaValidator::Validate(fake_schema, *parquet_metadata->schema());
  } catch (const std::exception& e) {
    EXPECT_TRUE(std::string_view(e.what()).find(expected_substring1) != std::string_view::npos);
    EXPECT_TRUE(std::string_view(e.what()).find(expected_substring2) != std::string_view::npos);
    return;
  }
  EXPECT_TRUE(false);
}

TEST(SchemaValidationTest, UUIDNeqString) {
  auto schema = GetMetadata(kAllTrinoTypesMetadata)->GetCurrentSchema();
  std::vector<iceberg::types::NestedField> fake_columns(schema->Columns());
  EXPECT_EQ(fake_columns[12].type->TypeId(), iceberg::TypeID::kUuid);
  fake_columns[12].type = std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kString);
  iceberg::Schema fake_schema(schema->SchemaId(), fake_columns);

  auto reader = parquet::ParquetFileReader::OpenFile(kAllTrinoTypesData);
  auto parquet_metadata = reader->metadata();

  const std::string expected_substring1 = LogicalTypeErrorMessage(
      std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kString), parquet::LogicalType::String());
  const std::string expected_substring2 =
      "Iceberg " + std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kString)->ToString() +
      " column must be encoded in UTF-8\n";
  const std::string expected_substring3 = PhysicalTypeErrorMessage(
      std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kString), parquet::Type::BYTE_ARRAY);
  try {
    iceberg::IcebergToParquetSchemaValidator::Validate(fake_schema, *parquet_metadata->schema());
  } catch (const std::exception& e) {
    EXPECT_TRUE(std::string_view(e.what()).find(expected_substring1) != std::string_view::npos);
    EXPECT_TRUE(std::string_view(e.what()).find(expected_substring2) != std::string_view::npos);
    EXPECT_TRUE(std::string_view(e.what()).find(expected_substring3) != std::string_view::npos);
    return;
  }
  EXPECT_TRUE(false);
}

TEST(SchemaValidationTest, StringIsNotJustBinary) {
  auto schema = GetMetadata(kAllSparkTypesMetadata)->GetCurrentSchema();

  std::vector<iceberg::types::NestedField> fake_columns(schema->Columns());
  EXPECT_EQ(fake_columns[7].type->TypeId(), iceberg::TypeID::kBinary);
  fake_columns[7].type = std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kString);
  iceberg::Schema fake_schema(schema->SchemaId(), fake_columns);

  auto reader = parquet::ParquetFileReader::OpenFile(kAllSparkTypesData);
  auto parquet_metadata = reader->metadata();

  const std::string expected_substring1 = LogicalTypeErrorMessage(
      std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kString), parquet::LogicalType::String());
  const std::string expected_substring2 =
      "Iceberg " + std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kString)->ToString() +
      " column must be encoded in UTF-8\n";
  try {
    iceberg::IcebergToParquetSchemaValidator::Validate(fake_schema, *parquet_metadata->schema());
  } catch (const std::exception& e) {
    EXPECT_TRUE(std::string_view(e.what()).find(expected_substring1) != std::string_view::npos);
    EXPECT_TRUE(std::string_view(e.what()).find(expected_substring2) != std::string_view::npos);
    return;
  }
  EXPECT_TRUE(false);
}

TEST(SchemaValidationTest, TimeNeqTimestamp) {
  auto schema = GetMetadata(kAllTrinoTypesMetadata)->GetCurrentSchema();
  std::vector<iceberg::types::NestedField> fake_columns(schema->Columns());
  EXPECT_EQ(fake_columns[8].type->TypeId(), iceberg::TypeID::kTime);
  fake_columns[8].type = std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kTimestamp);
  iceberg::Schema fake_schema(schema->SchemaId(), fake_columns);

  auto reader = parquet::ParquetFileReader::OpenFile(kAllTrinoTypesData);
  auto parquet_metadata = reader->metadata();

  const std::string expected_message =
      LogicalTypeErrorMessage(std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kTimestamp),
                              parquet::LogicalType::Timestamp(false, parquet::LogicalType::TimeUnit::MICROS));

  try {
    iceberg::IcebergToParquetSchemaValidator::Validate(fake_schema, *parquet_metadata->schema());
  } catch (const std::exception& e) {
    EXPECT_EQ(std::string_view(e.what()), expected_message);
    return;
  }
  EXPECT_TRUE(false);
}

/*TEST(SchemaValidationTest, UnsupportedTypes) {
 std::ifstream input(
     "tables/types/unsupported_types/metadata/00000-27b520d0-8020-43cb-b051-4eef7c1636af.metadata.json");

 std::stringstream ss;
 ss << input.rdbuf();
 std::string data = ss.str();

 auto metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
 EXPECT_NE(metadata, nullptr);

 auto reader = parquet::ParquetFileReader::OpenFile(
     "tables/types/unsupported_types/data/00007-7-40e35baf-8f61-4545-972a-8ef5acb5bd9c-0-00001.parquet");
 auto parquet_metadata = reader->metadata();
 EXPECT_THROW(
     iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(), *parquet_metadata->schema()),
     std::runtime_error);
}*/
