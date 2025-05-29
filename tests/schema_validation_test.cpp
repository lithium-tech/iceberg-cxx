#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "gtest/gtest.h"
#include "parquet/file_reader.h"

#include <fstream>

TEST(SchemaValidationTest, Decimal) {
    std::ifstream input("tables/decimal_partitioning/metadata/00002-1f7414b4-70da-433f-8a71-40192d2942fc.metadata.json");

  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  auto metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  EXPECT_NE(metadata, nullptr);

  auto reader = parquet::ParquetFileReader::OpenFile("tables/decimal_partitioning/data/col_decimal_9_2=3.00/col_decimal_18_2=-5.00/col_decimal_19_2=0.00/col_decimal_38_2=0.00/20250308_001617_00002_sdejt-955cdfa3-7763-48fc-87a2-ce832628f641.parquet");
  auto parquet_metadata = reader->metadata();
  EXPECT_NO_THROW(iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(), *parquet_metadata->schema()));
}

TEST(SchemaValidationTest, DecimalNoThrow) {
    std::ifstream input("tables/decimal_partitioning/metadata/00002-1f7414b4-70da-433f-8a71-40192d2942fc.metadata.json");

  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  auto metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  EXPECT_NE(metadata, nullptr);

  auto reader = parquet::ParquetFileReader::OpenFile("tables/decimal_partitioning/data/col_decimal_9_2=3.00/col_decimal_18_2=-5.00/col_decimal_19_2=0.00/col_decimal_38_2=0.00/20250308_001617_00002_sdejt-955cdfa3-7763-48fc-87a2-ce832628f641.parquet");
  auto parquet_metadata = reader->metadata();
  EXPECT_TRUE(iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(), *parquet_metadata->schema(), false));
}

TEST(SchemaValidationTest, AllSparkTypes) {
    std::ifstream input("tables/types/all_spark_types/metadata/00000-4f9d7c78-62f9-48c6-b869-f25172b45b32.metadata.json");

  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  auto metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
  EXPECT_NE(metadata, nullptr);

  auto reader = parquet::ParquetFileReader::OpenFile("tables/types/all_spark_types/data/00007-7-08e6240e-b5f7-48ad-b7f0-039dc52bc563-0-00001.parquet");
  auto parquet_metadata = reader->metadata();
  EXPECT_NO_THROW(iceberg::IcebergToParquetSchemaValidator::Validate(*metadata->GetCurrentSchema(), *parquet_metadata->schema()));
}
