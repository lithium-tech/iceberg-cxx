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
    metadata_builder->SetStatistics(str_stats);
  }
}

void CreateRowGroupMetaData(RowGroupMetaDataBuilder* row_group_builder, const Stats<std::string>& str_stats,
                            const Stats<int64_t>& int_stats) {
  CreateByteArrayColumnChunkMeta(row_group_builder->NextColumnChunk(), str_stats);

  CreateInt64ColumnChunkMeta(row_group_builder->NextColumnChunk(), int_stats);

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

using Result = PositionalDeleteStream::BasicRowGroupFilter::Result;

TEST(RowGroupFilter, NoStrStats) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{false, path, path}};
  std::vector<Stats<int64_t>> int_stats = {{true, 1, 2}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_EQ(filter.State(metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}), Result::kInter);
}

TEST(RowGroupFilter, NoIntStats) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{true, path, path}};
  std::vector<Stats<int64_t>> int_stats = {{false, 1, 2}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_EQ(filter.State(metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}), Result::kInter);
}

TEST(RowGroupFilter, PathOutOfRange) {
  std::vector<Stats<std::string>> str_stats = {{true, "b", "c"}};
  std::vector<Stats<int64_t>> int_stats = {{true, 1, 2}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_EQ(filter.State(metadata->RowGroup(0).get(), PositionalDeleteStream::Query{"a", 3, 5}), Result::kGreater);
  EXPECT_EQ(filter.State(metadata->RowGroup(0).get(), PositionalDeleteStream::Query{"d", 3, 5}), Result::kLess);
}

TEST(RowGroupFilter, NoIntMinMax) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{true, path, path}};
  std::vector<Stats<int64_t>> int_stats = {{true, std::nullopt, std::nullopt}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_EQ(filter.State(metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}), Result::kInter);
}

TEST(RowGroupFilter, ManyPaths) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{true, path, "b"}};
  std::vector<Stats<int64_t>> int_stats = {{true, 1, 2}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_EQ(filter.State(metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 3, 5}), Result::kInter);
}

TEST(RowGroupFilter, IntMinMax) {
  const std::string path = "a";

  std::vector<Stats<std::string>> str_stats = {{true, path, path}};
  std::vector<Stats<int64_t>> int_stats = {{true, 3, 5}};

  auto metadata = GetFileMetadata(str_stats, int_stats);
  PositionalDeleteStream::BasicRowGroupFilter filter;
  EXPECT_EQ(filter.State(metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 6, 7}), Result::kLess);
  EXPECT_EQ(filter.State(metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 2, 4}), Result::kInter);
  EXPECT_EQ(filter.State(metadata->RowGroup(0).get(), PositionalDeleteStream::Query{path, 1, 3}), Result::kGreater);
}

}  // namespace
