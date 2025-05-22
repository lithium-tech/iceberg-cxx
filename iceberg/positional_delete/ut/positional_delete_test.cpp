#include "iceberg/positional_delete/positional_delete.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "gtest/gtest.h"
#include "parquet/arrow/reader.h"
#include "parquet/column_writer.h"
#include "parquet/file_writer.h"

namespace iceberg {
namespace {
using arrow::Status;
using arrow::fs::FileSystem;
using parquet::LogicalType;
using parquet::ParquetFileWriter;
using parquet::Repetition;
using parquet::RowGroupWriter;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

struct DataStringInt64 {
  std::string file_path;
  int64_t pos;

  static std::shared_ptr<GroupNode> SetupSchema() {
    parquet::schema::NodeVector fields = {
        PrimitiveNode::Make("file_path", Repetition::REQUIRED, LogicalType::String(), Type::BYTE_ARRAY),
        PrimitiveNode::Make("pos", Repetition::REQUIRED, LogicalType::None(), Type::INT64)};
    return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }

  static void WriteToRowGroup(const std::vector<DataStringInt64>& row_group, RowGroupWriter* rg_writer) {
    auto file_path_writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
    for (const auto& row : row_group) {
      parquet::ByteArray file_path_value{row.file_path};
      file_path_writer->WriteBatch(1, nullptr, nullptr, &file_path_value);
    }

    auto pos_writer = static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
    for (const auto& row : row_group) {
      pos_writer->WriteBatch(1, nullptr, nullptr, &row.pos);
    }
  }
};

struct DataInt64Int64 {
  int64_t pos1;
  int64_t pos2;

  static std::shared_ptr<GroupNode> SetupSchema() {
    parquet::schema::NodeVector fields = {
        PrimitiveNode::Make("pos1", Repetition::REQUIRED, LogicalType::None(), Type::INT64),
        PrimitiveNode::Make("pos2", Repetition::REQUIRED, LogicalType::None(), Type::INT64)};
    return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }

  static void WriteToRowGroup(const std::vector<DataInt64Int64>& row_group, RowGroupWriter* rg_writer) {
    auto pos1_writer = static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
    for (const auto& row : row_group) {
      pos1_writer->WriteBatch(1, nullptr, nullptr, &row.pos1);
    }

    auto pos2_writer = static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
    for (const auto& row : row_group) {
      pos2_writer->WriteBatch(1, nullptr, nullptr, &row.pos2);
    }
  }
};

struct DataStringString {
  std::string file_path1;
  std::string file_path2;

  static std::shared_ptr<GroupNode> SetupSchema() {
    parquet::schema::NodeVector fields = {
        PrimitiveNode::Make("file_path1", Repetition::REQUIRED, LogicalType::String(), Type::BYTE_ARRAY),
        PrimitiveNode::Make("file_path2", Repetition::REQUIRED, LogicalType::String(), Type::BYTE_ARRAY)};
    return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }

  static void WriteToRowGroup(const std::vector<DataStringString>& row_group, RowGroupWriter* rg_writer) {
    auto file_path1_writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
    for (const auto& row : row_group) {
      parquet::ByteArray file_path_value{row.file_path1};
      file_path1_writer->WriteBatch(1, nullptr, nullptr, &file_path_value);
    }

    auto file_path2_writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
    for (const auto& row : row_group) {
      parquet::ByteArray file_path_value{row.file_path2};
      file_path2_writer->WriteBatch(1, nullptr, nullptr, &file_path_value);
    }
  }
};

struct DataInt64 {
  int64_t pos;

  static std::shared_ptr<GroupNode> SetupSchema() {
    parquet::schema::NodeVector fields = {
        PrimitiveNode::Make("pos", Repetition::REQUIRED, LogicalType::None(), Type::INT64)};
    return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }

  static void WriteToRowGroup(const std::vector<DataInt64>& row_group, RowGroupWriter* rg_writer) {
    auto pos_writer = static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
    for (const auto& row : row_group) {
      pos_writer->WriteBatch(1, nullptr, nullptr, &row.pos);
    }
  }
};

template <typename Data>
void WriteToFile(const std::vector<std::vector<Data>>& row_groups, ParquetFileWriter* writer) {
  for (const auto& row_group : row_groups) {
    auto rg_writer = writer->AppendRowGroup();

    Data::WriteToRowGroup(row_group, rg_writer);
  }
}

template <typename Data>
arrow::Status WriteFile(std::shared_ptr<FileSystem> fs, const std::string& path,
                        const std::vector<std::vector<Data>>& row_groups) {
  ARROW_ASSIGN_OR_RAISE(auto out_stream, fs->OpenOutputStream(path));
  auto schema = Data::SetupSchema();
  auto file_writer = parquet::ParquetFileWriter::Open(out_stream, schema);
  WriteToFile(row_groups, file_writer.get());
  file_writer->Close();
  return out_stream->Close();
}

arrow::Result<std::unique_ptr<parquet::arrow::FileReader>> OpenUrl(std::shared_ptr<FileSystem> fs,
                                                                   const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto input_file, fs->OpenInputFile(path));

  parquet::arrow::FileReaderBuilder reader_builder;
  ARROW_RETURN_NOT_OK(reader_builder.Open(input_file));
  reader_builder.memory_pool(arrow::default_memory_pool());
  reader_builder.properties(parquet::default_arrow_reader_properties());

  return reader_builder.Build();
}

Status ReadFile(std::shared_ptr<FileSystem> fs, const std::string& path, UrlDeleteRows& rows,
                uint64_t* count = nullptr) {
  try {
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_ASSIGN_OR_RAISE(arrow_reader, OpenUrl(fs, path));

    PositionalDeleteStream(std::move(arrow_reader), 0).Append(rows);

    if (count) {
      *count = 0;
      for (const auto& [_, poses] : rows) {
        *count += poses.size();
      }
    }
  } catch (const arrow::Status& status) {
    return status;
  }
  return arrow::Status::OK();
}

class PositionalDeleteTest : public ::testing::Test {
 protected:
  void SetUp() override { fs_ = arrow::fs::FileSystemFromUri(uri_, &path_).ValueOrDie(); }

  const std::string uri_ = "mock:///delete.parquet";
  std::string path_;
  std::shared_ptr<FileSystem> fs_;
};

TEST_F(PositionalDeleteTest, ReadEmpty) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{}), Status::OK());
  UrlDeleteRows rows;
  ASSERT_EQ(ReadFile(fs_, path_, rows), Status::OK());
  EXPECT_EQ(rows, UrlDeleteRows{});
}

TEST_F(PositionalDeleteTest, ReadMixedPath) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path2", 3}}}), Status::OK());
  UrlDeleteRows rows;
  ASSERT_EQ(ReadFile(fs_, path_, rows), arrow::Status::OK());
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1}}, {"path2", {3}}}));
}

TEST_F(PositionalDeleteTest, ReadMultipleRowGroups) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{"path1", 1}}, {{"path1", 3}}}), Status::OK());
  UrlDeleteRows rows;
  ASSERT_EQ(ReadFile(fs_, path_, rows), arrow::Status::OK());
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1, 3}}}));
}

TEST_F(PositionalDeleteTest, ReadUpdate) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{"path1", 3}}}), Status::OK());
  UrlDeleteRows rows = {{"path1", {1}}};
  ASSERT_EQ(ReadFile(fs_, path_, rows), arrow::Status::OK());
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1, 3}}}));
}

TEST_F(PositionalDeleteTest, MergeUpdates) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{"path1", 3}, {"path1", 6}}}), Status::OK());
  UrlDeleteRows rows = {{"path1", {1, 4, 5}}, {"path2", {2}}};
  ASSERT_EQ(ReadFile(fs_, path_, rows), arrow::Status::OK());
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1, 3, 4, 5, 6}}, {"path2", {2}}}));
}

TEST_F(PositionalDeleteTest, WrongColumnNumber) {
  using Container = std::vector<std::vector<DataInt64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{3}}}), Status::OK());
  UrlDeleteRows rows;
  ASSERT_NE(ReadFile(fs_, path_, rows), arrow::Status::OK());
}

TEST_F(PositionalDeleteTest, WrongFirstColumnType) {
  using Container = std::vector<std::vector<DataInt64Int64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{3, 25}}}), Status::OK());
  UrlDeleteRows rows;
  ASSERT_NE(ReadFile(fs_, path_, rows), arrow::Status::OK());
}

TEST_F(PositionalDeleteTest, WrongSecondColumnType) {
  using Container = std::vector<std::vector<DataStringString>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{"aba", "caba"}}}), Status::OK());
  UrlDeleteRows rows;
  ASSERT_NE(ReadFile(fs_, path_, rows), arrow::Status::OK());
}

TEST_F(PositionalDeleteTest, GetDeletedFromStream) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path1", 3}, {"path1", 4}}}), Status::OK());

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader = OpenUrl(fs_, path_).ValueOrDie();

  PositionalDeleteStream stream(std::move(arrow_reader), 0);
  ASSERT_EQ(stream.GetDeleted("path1", 1, 2, 0), (DeleteRows{1}));
  ASSERT_EQ(stream.GetDeleted("path1", 2, 3, 0), (DeleteRows{}));
  ASSERT_EQ(stream.GetDeleted("path1", 3, 5, 0), (DeleteRows{{3, 4}}));
}

TEST_F(PositionalDeleteTest, GetDeletedFromStreamWithSkips) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path1", 3}, {"path2", 4}}}), Status::OK());

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader = OpenUrl(fs_, path_).ValueOrDie();

  PositionalDeleteStream stream(std::move(arrow_reader), 0);
  ASSERT_EQ(stream.GetDeleted("path1", 1, 2, 0), (DeleteRows{1}));
  ASSERT_EQ(stream.GetDeleted("path1", 2, 3, 0), (DeleteRows{}));
  ASSERT_EQ(stream.GetDeleted("path2", 3, 5, 0), (DeleteRows{4}));
}

TEST_F(PositionalDeleteTest, Count) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path2", 3}}}), Status::OK());
  UrlDeleteRows rows;
  uint64_t count = 0;
  ASSERT_EQ(ReadFile(fs_, path_, rows, &count), arrow::Status::OK());
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1}}, {"path2", {3}}}));
  EXPECT_EQ(count, 2);
}

TEST_F(PositionalDeleteTest, DoNotCountTwice) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_EQ(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path2", 3}}}), Status::OK());
  UrlDeleteRows rows{{"path1", {1}}};
  uint64_t count = 1;
  ASSERT_EQ(ReadFile(fs_, path_, rows, &count), arrow::Status::OK());
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1}}, {"path2", {3}}}));
  EXPECT_EQ(count, 2);
}

}  // namespace
}  // namespace iceberg
