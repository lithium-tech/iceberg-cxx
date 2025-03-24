#include <arrow/filesystem/localfs.h>
#include <arrow/io/file.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>

#include <limits>
#include <memory>
#include <stdexcept>

#include "gtest/gtest.h"
#include "iceberg/schema.h"
#include "iceberg/sql_catalog.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transforms.h"

namespace iceberg {

namespace {

std::shared_ptr<arrow::Table> ReadTableFromParquet(const std::string& file_path) {
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  auto maybe_input_file = fs->OpenInputFile(file_path);
  if (!maybe_input_file.ok()) {
    throw maybe_input_file.status();
  }
  auto input_file = maybe_input_file.MoveValueUnsafe();

  parquet::arrow::FileReaderBuilder reader_builder;
  if (auto status = reader_builder.Open(input_file); !status.ok()) {
    throw status;
  }
  reader_builder.memory_pool(arrow::default_memory_pool());
  reader_builder.properties(parquet::default_arrow_reader_properties());

  auto maybe_arrow_reader = reader_builder.Build();
  if (!maybe_arrow_reader.ok()) {
    throw maybe_arrow_reader.status();
  }
  auto arrow_reader = maybe_arrow_reader.MoveValueUnsafe();

  std::shared_ptr<arrow::Table> table;
  if (auto status = arrow_reader->ReadTable(&table); !status.ok()) {
    throw status;
  }

  return table;
}

Schema MakeIcebergSchemaFromArrow(int schema_id, const std::shared_ptr<arrow::Schema>& arrow_schema) {
  std::vector<types::NestedField> fields;
  for (size_t i = 0; i < arrow_schema->fields().size(); ++i) {
    const auto& field = arrow_schema->field(i);
    fields.push_back(types::NestedField{.name = field->name(),
                                        .field_id = static_cast<int>(i),
                                        .is_required = true,
                                        .type = types::ConvertArrowTypeToIceberg(field->type(), i)});
  }
  return Schema(schema_id, std::move(fields));
}

std::shared_ptr<TableMetadataV2> DefaultTableMetadata() {
  return std::make_shared<iceberg::TableMetadataV2>(
      std::string{"1"}, std::string{"1"}, 0, 0, 0,
      std::vector<std::shared_ptr<iceberg::Schema>>{
          std::make_shared<iceberg::Schema>(0, std::vector<types::NestedField>{})},
      0,
      std::vector<std::shared_ptr<iceberg::PartitionSpec>>{
          std::make_shared<iceberg::PartitionSpec>(iceberg::PartitionSpec{0, std::vector<iceberg::PartitionField>{}})},
      0, 0, std::map<std::string, std::string>{}, std::nullopt, std::vector<std::shared_ptr<iceberg::Snapshot>>{},
      std::vector<iceberg::SnapshotLog>{}, std::vector<iceberg::MetadataLog>{},
      std::vector<std::shared_ptr<iceberg::SortOrder>>{
          std::make_shared<iceberg::SortOrder>(iceberg::SortOrder{0, std::vector<iceberg::SortField>{}})},
      0, std::map<std::string, iceberg::SnapshotRef>{});
  ;
}

}  // namespace

TEST(CompatPartitions, NoPartitioning) {
  auto parquet_table = ReadTableFromParquet("data/00000-7-d4e36f4d-a2c0-467d-90e7-0ef1a54e2724-0-00002.parquet");

  SqlCatalog catalog("//tmp/cppcatalog", std::make_shared<arrow::fs::LocalFileSystem>());
  auto table = catalog.CreateTable(catalog::TableIdentifier{.db = "db", .name = "test_table1"},
                                   MakeIcebergSchemaFromArrow(0, parquet_table->schema()));

  table->AppendTable(parquet_table);
  EXPECT_EQ(table->GetFilePathes().size(), 1);
}

TEST(CompatPartitions, IdentityPartitioning) {
  auto parquet_table = ReadTableFromParquet("data/00000-7-d4e36f4d-a2c0-467d-90e7-0ef1a54e2724-0-00002.parquet");

  std::shared_ptr<TableMetadataV2> table_metadata = DefaultTableMetadata();
  PartitionSpec partition_spec{.spec_id = 0,
                               .fields = {PartitionField{.source_id = static_cast<int32_t>(0),
                                                         .field_id = static_cast<int32_t>(0),
                                                         .name = "r_regionkey",
                                                         .transform = "identity"}}};

  table_metadata->partition_specs = {std::make_shared<PartitionSpec>(partition_spec)};

  SqlCatalog catalog("//tmp/cppcatalog", std::make_shared<arrow::fs::LocalFileSystem>());
  auto table = catalog.CreateTable(catalog::TableIdentifier{.db = "db", .name = "test_table2"},
                                   MakeIcebergSchemaFromArrow(0, parquet_table->schema()), table_metadata);

  table->AppendTable(parquet_table);
  EXPECT_EQ(table->GetFilePathes().size(), 5);
}

TEST(CompatPartitions, BucketPartitioning) {
  auto parquet_table = ReadTableFromParquet("data/00000-7-d4e36f4d-a2c0-467d-90e7-0ef1a54e2724-0-00002.parquet");

  std::shared_ptr<TableMetadataV2> table_metadata = DefaultTableMetadata();
  PartitionSpec partition_spec{.spec_id = 0,
                               .fields = {PartitionField{.source_id = static_cast<int32_t>(0),
                                                         .field_id = static_cast<int32_t>(0),
                                                         .name = "n_regionkey",
                                                         .transform = "bucket[3]"}}};

  table_metadata->partition_specs = {std::make_shared<PartitionSpec>(partition_spec)};

  SqlCatalog catalog("//tmp/cppcatalog", std::make_shared<arrow::fs::LocalFileSystem>());
  auto table = catalog.CreateTable(catalog::TableIdentifier{.db = "db", .name = "test_table3"},
                                   MakeIcebergSchemaFromArrow(0, parquet_table->schema()), table_metadata);

  table->AppendTable(parquet_table);
#ifdef USE_SMHASHER
  EXPECT_EQ(table->GetFilePathes().size(), 2);
#else
  EXPECT_EQ(table->GetFilePathes().size(), 3);
#endif
}

TEST(CompatPartitions, TruncateTransform) {
  auto parquet_table = ReadTableFromParquet("data/00000-7-d4e36f4d-a2c0-467d-90e7-0ef1a54e2724-0-00002.parquet");

  std::shared_ptr<TableMetadataV2> table_metadata = DefaultTableMetadata();
  PartitionSpec partition_spec{.spec_id = 0,
                               .fields = {PartitionField{.source_id = static_cast<int32_t>(0),
                                                         .field_id = static_cast<int32_t>(0),
                                                         .name = "n_regionkey",
                                                         .transform = "truncate[3]"}}};

  table_metadata->partition_specs = {std::make_shared<PartitionSpec>(partition_spec)};

  SqlCatalog catalog("//tmp/cppcatalog", std::make_shared<arrow::fs::LocalFileSystem>());
  auto table = catalog.CreateTable(catalog::TableIdentifier{.db = "db", .name = "test_table4"},
                                   MakeIcebergSchemaFromArrow(0, parquet_table->schema()), table_metadata);

  table->AppendTable(parquet_table);
#ifdef USE_SMHASHER
  EXPECT_EQ(table->GetFilePathes().size(), 5);
#else
  EXPECT_EQ(table->GetFilePathes().size(), 3);
#endif
}

}  // namespace iceberg
