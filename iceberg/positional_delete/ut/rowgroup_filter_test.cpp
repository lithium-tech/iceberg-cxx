#include <optional>

#include <parquet/column_page.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include "gtest/gtest.h"
#include "iceberg/positional_delete/positional_delete.h"
#include "iceberg/schema.h"

namespace {
using namespace parquet;
using namespace iceberg;

template <typename T>
struct Stats {
  bool set_stats;
  std::optional<T> min_val;
  std::optional<T> max_val;
  std::optional<int64_t> distinct_count;
};

void CreateInt64ColumnChunkMeta(ColumnChunkMetaDataBuilder* metadata_builder, const Stats<int64_t>& stats) {
  auto node = schema::PrimitiveNode::Make("int64_col", Repetition::REQUIRED, Type::INT64);
  ColumnDescriptor descriptor(node, 0, 0);

  if (stats.set_stats) {
    EncodedStatistics int_stats;
    if (stats.min_val.has_value()) {
      int_stats.set_min(std::string(reinterpret_cast<const char*>(&stats.min_val.value()), sizeof(int64_t)));
    }
    if (stats.max_val.has_value()) {
      int_stats.set_max(std::string(reinterpret_cast<const char*>(&stats.max_val.value()), sizeof(int64_t)));
    }
    if (stats.distinct_count.has_value()) {
      int_stats.set_distinct_count(stats.distinct_count.value());
    }
    metadata_builder->SetStatistics(int_stats);
  }
}

void CreateByteArrayColumnChunkMeta(ColumnChunkMetaDataBuilder* metadata_builder, const Stats<std::string>& stats) {
  auto node = schema::PrimitiveNode::Make("bytearray_col", Repetition::REQUIRED, Type::BYTE_ARRAY);
  ColumnDescriptor descriptor(node, 0, 0);
  if (stats.set_stats) {
    EncodedStatistics str_stats;
    if (stats.min_val.has_value()) {
      str_stats.set_min(stats.min_val.value());
    }
    if (stats.max_val.has_value()) {
      str_stats.set_max(stats.max_val.value());
    }
    if (stats.distinct_count.has_value()) {
      str_stats.set_distinct_count(stats.distinct_count.value());
    }
    metadata_builder->SetStatistics(str_stats);
  }
}

void CreateRowGroupMetaData(RowGroupMetaDataBuilder* row_group_builder, const Stats<std::string>& str_stats,
                            const Stats<int64_t>& int_stats) {
  CreateByteArrayColumnChunkMeta(row_group_builder->NextColumnChunk(), str_stats);

  CreateInt64ColumnChunkMeta(row_group_builder->NextColumnChunk(), int_stats);

  row_group_builder->set_num_rows(1000);

  row_group_builder->Finish(4096);
}

std::shared_ptr<FileMetaData> GetFileMetadata(const std::vector<Stats<std::string>>& str_stats,
                                              const std::vector<Stats<int64_t>>& int_stats) {
  auto fields = {schema::PrimitiveNode::Make("name", Repetition::OPTIONAL, Type::BYTE_ARRAY),
                 schema::PrimitiveNode::Make("id", Repetition::REQUIRED, Type::INT64)};
  SchemaDescriptor schema_descriptor;
  schema_descriptor.Init(schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));

  auto props = WriterProperties::Builder().build();
  auto meta_builder = FileMetaDataBuilder::Make(&schema_descriptor, props);

  for (size_t i = 0; i < str_stats.size(); ++i) {
    CreateRowGroupMetaData(meta_builder->AppendRowGroup(), str_stats[i], int_stats[i]);
  }
  return meta_builder->Finish();
}

TEST(RowGroupFilter, OnePathDistinctCount) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{true, std::nullopt, std::nullopt, 1}};
  std::vector<Stats<int64_t>> int_stats = {{true, 1, 2, std::nullopt}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_TRUE(filter.Skip(path, metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}));
  EXPECT_FALSE(filter.Skip(path, metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 2, 5}));
  EXPECT_TRUE(filter.Skip(path, metadata->RowGroup(0).get(), PositionalDeleteStream::Query{"b", 2, 5}));
}

TEST(RowGroupFilter, ManyPathsDistinctCount) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{true, std::nullopt, std::nullopt, 2}};
  std::vector<Stats<int64_t>> int_stats = {{true, 1, 2, std::nullopt}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_FALSE(filter.Skip(path, metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}));
}

TEST(RowGroupFilter, OnePathMinMax) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{true, path, path, std::nullopt}};
  std::vector<Stats<int64_t>> int_stats = {{true, 1, 2, std::nullopt}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_TRUE(filter.Skip(path, metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}));
}

TEST(RowGroupFilter, ManyPathsMinMax) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{true, path, "b", std::nullopt}};
  std::vector<Stats<int64_t>> int_stats = {{true, 1, 2, std::nullopt}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_FALSE(filter.Skip(path, metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}));
}

TEST(RowGroupFilter, NoMinMax) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{true, path, path, 1}};
  std::vector<Stats<int64_t>> int_stats = {{true, std::nullopt, std::nullopt, 1}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_FALSE(filter.Skip(path, metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}));
}

TEST(RowGroupFilter, NoStrStats) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{false, path, path, 1}};
  std::vector<Stats<int64_t>> int_stats = {{true, 1, 2, 1}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_FALSE(filter.Skip(path, metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}));
}

TEST(RowGroupFilter, NoIntStats) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{true, path, path, 1}};
  std::vector<Stats<int64_t>> int_stats = {{false, 1, 2, 1}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_FALSE(filter.Skip(path, metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}));
}

}  // namespace
