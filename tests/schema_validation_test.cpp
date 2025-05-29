#include <fstream>

#include "gtest/gtest.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "parquet/file_reader.h"

TEST(SchemaValidationTest, Decimal) {
  std::ifstream input("tables/types/decimals/metadata/00000-93fb1a3e-cf50-4083-af65-68d07b84c5ad.metadata.json");

  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  auto metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  EXPECT_NE(metadata, nullptr);

  auto reader = parquet::ParquetFileReader::OpenFile(
      "tables/types/decimals/data/00007-7-7b64d752-fbdc-48ca-b1a2-b88f692557ff-0-00001.parquet");
  auto parquet_metadata = reader->metadata();
  EXPECT_NO_THROW(
      iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(), *parquet_metadata->schema()));
}

TEST(SchemaValidationTest, DecimalNoThrow) {
  std::ifstream input("tables/types/decimals/metadata/00000-93fb1a3e-cf50-4083-af65-68d07b84c5ad.metadata.json");

  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  auto metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  EXPECT_NE(metadata, nullptr);

  auto reader = parquet::ParquetFileReader::OpenFile(
      "tables/types/decimals/data/00007-7-7b64d752-fbdc-48ca-b1a2-b88f692557ff-0-00001.parquet");
  auto parquet_metadata = reader->metadata();
  EXPECT_TRUE(iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(),
                                                                 *parquet_metadata->schema(), false));
}

TEST(SchemaValidationTest, AllSparkTypes) {
  std::ifstream input("tables/types/all_spark_types/metadata/00000-4f9d7c78-62f9-48c6-b869-f25172b45b32.metadata.json");

  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  auto metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  EXPECT_NE(metadata, nullptr);

  auto reader = parquet::ParquetFileReader::OpenFile(
      "tables/types/all_spark_types/data/00007-7-08e6240e-b5f7-48ad-b7f0-039dc52bc563-0-00001.parquet");
  auto parquet_metadata = reader->metadata();
  EXPECT_NO_THROW(
      iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(), *parquet_metadata->schema()));
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
