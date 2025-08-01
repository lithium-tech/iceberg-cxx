#include "iceberg/positional_delete/positional_delete.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "gtest/gtest.h"
#include "iceberg/common/fs/filesystem_wrapper.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "parquet/arrow/reader.h"
#include "parquet/column_reader.h"
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
        PrimitiveNode::Make(column_names[0], Repetition::REQUIRED, LogicalType::String(), Type::BYTE_ARRAY),
        PrimitiveNode::Make(column_names[1], Repetition::REQUIRED, LogicalType::None(), Type::INT64)};
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

  static const std::vector<std::string> column_names;
};

const std::vector<std::string> DataStringInt64::column_names = {"file_path", "pos"};

struct DataInt64Int64 {
  int64_t pos1;
  int64_t pos2;

  static std::shared_ptr<GroupNode> SetupSchema() {
    parquet::schema::NodeVector fields = {
        PrimitiveNode::Make(column_names[0], Repetition::REQUIRED, LogicalType::None(), Type::INT64),
        PrimitiveNode::Make(column_names[1], Repetition::REQUIRED, LogicalType::None(), Type::INT64)};
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

  static const std::vector<std::string> column_names;
};

const std::vector<std::string> DataInt64Int64::column_names = {"pos1", "pos2"};

struct DataStringString {
  std::string file_path1;
  std::string file_path2;

  static std::shared_ptr<GroupNode> SetupSchema() {
    parquet::schema::NodeVector fields = {
        PrimitiveNode::Make(column_names[0], Repetition::REQUIRED, LogicalType::String(), Type::BYTE_ARRAY),
        PrimitiveNode::Make(column_names[1], Repetition::REQUIRED, LogicalType::String(), Type::BYTE_ARRAY)};
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

  static const std::vector<std::string> column_names;
};

const std::vector<std::string> DataStringString::column_names = {"file_path1", "file_path2"};

struct DataInt64 {
  int64_t pos;

  static std::shared_ptr<GroupNode> SetupSchema() {
    parquet::schema::NodeVector fields = {
        PrimitiveNode::Make(column_names[0], Repetition::REQUIRED, LogicalType::None(), Type::INT64)};
    return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }

  static void WriteToRowGroup(const std::vector<DataInt64>& row_group, RowGroupWriter* rg_writer) {
    auto pos_writer = static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
    for (const auto& row : row_group) {
      pos_writer->WriteBatch(1, nullptr, nullptr, &row.pos);
    }
  }

  static const std::vector<std::string> column_names;
};

const std::vector<std::string> DataInt64::column_names = {"pos"};

template <typename Data>
void WriteToFile(const std::vector<std::vector<Data>>& row_groups, ParquetFileWriter* writer) {
  for (const auto& row_group : row_groups) {
    auto rg_writer = writer->AppendRowGroup();

    Data::WriteToRowGroup(row_group, rg_writer);
  }
}

template <typename Data>
arrow::Status WriteFile(std::shared_ptr<FileSystem> fs, const std::string& path,
                        const std::vector<std::vector<Data>>& row_groups, bool set_statistics = true) {
  ARROW_ASSIGN_OR_RAISE(auto out_stream, fs->OpenOutputStream(path));
  auto schema = Data::SetupSchema();

  auto props_builder = parquet::WriterProperties::Builder();
  props_builder.enable_statistics();
  if (!set_statistics) {
    for (const auto& name : Data::column_names) {
      props_builder.disable_statistics(name);
    }
  }
  auto file_writer = parquet::ParquetFileWriter::Open(out_stream, schema, props_builder.build());
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

template <typename Reader, typename Value>
void ReadValueNotNull(Reader& reader, Value& value) {
  int64_t values_read;
  int64_t rows_read = reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
  if (rows_read != 1 || values_read != 1) {
    throw arrow::Status::OK();
  }
}

std::string StringFromByteArray(const parquet::ByteArray& value) noexcept {
  return {reinterpret_cast<const char*>(value.ptr), value.len};
}

std::function<std::shared_ptr<parquet::arrow::FileReader>(const std::string&)> MakeCB(std::shared_ptr<FileSystem> fs) {
  return [fs](const std::string& url) {
    return std::shared_ptr<parquet::arrow::FileReader>(OpenUrl(fs, url).ValueOrDie().release());
  };
}

PositionalDeleteStream MakeStream(std::shared_ptr<FileSystem> fs, const std::string& path, int layer) {
  return PositionalDeleteStream({{layer, {path}}}, MakeCB(fs));
}

Status ReadFile(std::shared_ptr<FileSystem> fs, const std::string& path, UrlDeleteRows& rows,
                uint64_t* count = nullptr) {
  try {
    auto pos_del_stream = MakeStream(fs, path, 0);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader_local;
    ARROW_ASSIGN_OR_RAISE(arrow_reader_local, OpenUrl(fs, path));

    auto parquet_reader = arrow_reader_local->parquet_reader();

    std::set<std::string> paths;

    for (int i = 0; i < arrow_reader_local->num_row_groups(); ++i) {
      auto row_group_reader = parquet_reader->RowGroup(i);
      auto column0 = row_group_reader->Column(0);
      auto column1 = row_group_reader->Column(1);

      // Validate type of the columns.
      if (column0->type() != parquet::Type::BYTE_ARRAY) {
        throw arrow::Status::OK();
      }
      if (column1->type() != parquet::Type::INT64) {
        throw arrow::Status::OK();
      }

      auto file_path_reader = std::static_pointer_cast<parquet::ByteArrayReader>(column0);

      while (file_path_reader->HasNext()) {
        parquet::ByteArray file_path_value;
        ReadValueNotNull(file_path_reader, file_path_value);

        paths.insert(StringFromByteArray(file_path_value));
      }
    }

    for (const auto& path : paths) {
      const auto& poses = pos_del_stream.GetDeleted(path, 0, INT64_MAX, 0);
      rows[path].insert(rows[path].end(), poses.begin(), poses.end());
    }

    for (auto& [_, poses] : rows) {
      std::sort(poses.begin(), poses.end());
      // Remove possible duplicates.
      poses.erase(std::unique(poses.begin(), poses.end()), poses.end());
    }

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

struct Metrics {
  uint64_t requests = 0;
  uint64_t bytes_read = 0;
  uint64_t files_opened = 0;

  bool operator==(const Metrics& other) const = default;
};

class LoggingInputFile : public InputFileWrapper {
 public:
  LoggingInputFile(std::shared_ptr<arrow::io::RandomAccessFile> file, Metrics& metrics)
      : InputFileWrapper(file), metrics_(metrics) {}

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::ReadAt(position, nbytes, out);
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes, out);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes);
  }

 private:
  void TakeRequestIntoAccount(int64_t bytes) {
    ++metrics_.requests;
    metrics_.bytes_read += bytes;
  }

  Metrics& metrics_;
};

class LoggingFileSystem : public arrow::fs::LocalFileSystem {
 public:
  LoggingFileSystem() {}

  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(const std::string& path) override {
    ++metrics_.files_opened;
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::fs::LocalFileSystem::OpenInputFile(path));
    return std::make_shared<LoggingInputFile>(file, metrics_);
  }

  Metrics GetCurrentMetrics() const { return metrics_; }

  void Clear() { metrics_ = {}; }

 private:
  Metrics metrics_{};
};

class PositionalDeleteTest : public ::testing::Test {
 protected:
  void SetUp() override { fs_ = arrow::fs::FileSystemFromUri(uri_, &path_).ValueOrDie(); }

  const std::string uri_ = "mock:///delete.parquet";
  std::string path_;
  std::shared_ptr<FileSystem> fs_;
};

class PositionalDeleteTestLogging : public ::testing::Test {
 protected:
  void SetUp() override { logging_fs_ = std::make_shared<LoggingFileSystem>(); }

  std::string MakePath(const std::string& file_name) { return (dir_.path() / file_name).generic_string(); }

  ScopedTempDir dir_;
  std::shared_ptr<LoggingFileSystem> logging_fs_;
};

TEST_F(PositionalDeleteTest, ReadEmpty) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{}));
  UrlDeleteRows rows;
  ASSERT_OK(ReadFile(fs_, path_, rows));
  EXPECT_EQ(rows, UrlDeleteRows{});
}

TEST_F(PositionalDeleteTest, ReadMixedPath) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path2", 3}}}));
  UrlDeleteRows rows;
  ASSERT_OK(ReadFile(fs_, path_, rows));
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1}}, {"path2", {3}}}));
}

TEST_F(PositionalDeleteTest, ReadMultipleRowGroups) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"path1", 1}}, {{"path1", 3}}}));
  UrlDeleteRows rows;
  ASSERT_OK(ReadFile(fs_, path_, rows));
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1, 3}}}));
}

TEST_F(PositionalDeleteTest, ReadUpdate) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"path1", 3}}}));
  UrlDeleteRows rows = {{"path1", {1}}};
  ASSERT_OK(ReadFile(fs_, path_, rows));
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1, 3}}}));
}

TEST_F(PositionalDeleteTest, MergeUpdates) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"path1", 3}, {"path1", 6}}}));
  UrlDeleteRows rows = {{"path1", {1, 4, 5}}, {"path2", {2}}};
  ASSERT_OK(ReadFile(fs_, path_, rows));
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1, 3, 4, 5, 6}}, {"path2", {2}}}));
}

TEST_F(PositionalDeleteTest, WrongColumnNumber) {
  using Container = std::vector<std::vector<DataInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{3}}}));
  UrlDeleteRows rows;
  ASSERT_NE(ReadFile(fs_, path_, rows), arrow::Status::OK());
}

TEST_F(PositionalDeleteTest, WrongFirstColumnType) {
  using Container = std::vector<std::vector<DataInt64Int64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{3, 25}}}));
  UrlDeleteRows rows;
  ASSERT_NE(ReadFile(fs_, path_, rows), arrow::Status::OK());
}

TEST_F(PositionalDeleteTest, WrongSecondColumnType) {
  using Container = std::vector<std::vector<DataStringString>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"aba", "caba"}}}));
  UrlDeleteRows rows;
  ASSERT_NE(ReadFile(fs_, path_, rows), arrow::Status::OK());
}

TEST_F(PositionalDeleteTest, GetDeletedFromStream) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path1", 3}, {"path1", 4}}}));

  auto stream = MakeStream(fs_, path_, 0);
  ASSERT_EQ(stream.GetDeleted("path1", 1, 2, 0), (DeleteRows{1}));
  ASSERT_EQ(stream.GetDeleted("path1", 2, 3, 0), (DeleteRows{}));
  ASSERT_EQ(stream.GetDeleted("path1", 3, 5, 0), (DeleteRows{{3, 4}}));
}

TEST_F(PositionalDeleteTest, GetDeletedFromStreamLayerGreater) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path1", 3}, {"path1", 4}}}));

  auto stream = MakeStream(fs_, path_, 1);
  ASSERT_EQ(stream.GetDeleted("path1", 1, 2, 0), (DeleteRows{1}));
  ASSERT_EQ(stream.GetDeleted("path1", 2, 3, 0), (DeleteRows{}));
  ASSERT_EQ(stream.GetDeleted("path1", 3, 5, 0), (DeleteRows{{3, 4}}));
}

TEST_F(PositionalDeleteTest, GetDeletedFromStreamLayerLess) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path1", 3}, {"path1", 4}}}));

  auto stream = MakeStream(fs_, path_, 0);
  ASSERT_EQ(stream.GetDeleted("path1", 1, 2, 1), (DeleteRows{}));
  ASSERT_EQ(stream.GetDeleted("path1", 2, 3, 1), (DeleteRows{}));
  ASSERT_EQ(stream.GetDeleted("path1", 3, 5, 1), (DeleteRows{}));
}

TEST_F(PositionalDeleteTest, GetDeletedFromStreamWithSkips) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path1", 3}, {"path2", 4}}}));

  auto stream = MakeStream(fs_, path_, 0);
  ASSERT_EQ(stream.GetDeleted("path1", 1, 2, 0), (DeleteRows{1}));
  ASSERT_EQ(stream.GetDeleted("path1", 2, 3, 0), (DeleteRows{}));
  ASSERT_EQ(stream.GetDeleted("path2", 3, 5, 0), (DeleteRows{4}));
}

TEST_F(PositionalDeleteTest, Count) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path2", 3}}}));
  UrlDeleteRows rows;
  uint64_t count = 0;
  ASSERT_OK(ReadFile(fs_, path_, rows, &count));
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1}}, {"path2", {3}}}));
  EXPECT_EQ(count, 2);
}

TEST_F(PositionalDeleteTest, DoNotCountTwice) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(WriteFile(fs_, path_, Container{{{"path1", 1}, {"path2", 3}}}));
  UrlDeleteRows rows{{"path1", {1}}};
  uint64_t count = 1;
  ASSERT_OK(ReadFile(fs_, path_, rows, &count));
  EXPECT_EQ(rows, (UrlDeleteRows{{"path1", {1}}, {"path2", {3}}}));
  EXPECT_EQ(count, 2);
}

TEST_F(PositionalDeleteTest, NoStats) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(fs_->CreateDir(path_));

  ASSERT_OK(WriteFile(fs_, path_ + "/first", Container{{{"path1", 1}}}, false));
  ASSERT_OK(WriteFile(fs_, path_ + "/second", Container{{{"path2", 2}}}, false));

  std::map<int, std::vector<std::string>> urls = {{0, {path_ + "/second", path_ + "/first"}}};
  EXPECT_EQ(PositionalDeleteStream(urls, MakeCB(fs_)).GetDeleted("path1", 0, 2, 0), DeleteRows{1});
  EXPECT_EQ(PositionalDeleteStream(urls, MakeCB(fs_)).GetDeleted("path2", 1, 3, 0), DeleteRows{2});
}

TEST_F(PositionalDeleteTest, MixedStats1) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(fs_->CreateDir(path_));

  ASSERT_OK(WriteFile(fs_, path_ + "/first", Container{{{"path1", 1}}}, false));
  ASSERT_OK(WriteFile(fs_, path_ + "/second", Container{{{"path2", 2}}}));
  ASSERT_OK(WriteFile(fs_, path_ + "/third", Container{{{"path3", 3}}}, false));

  std::map<int, std::vector<std::string>> urls = {{0, {path_ + "/first", path_ + "/second", path_ + "/third"}}};
  EXPECT_EQ(PositionalDeleteStream(urls, MakeCB(fs_)).GetDeleted("path1", 0, 2, 0), DeleteRows{1});
  EXPECT_EQ(PositionalDeleteStream(urls, MakeCB(fs_)).GetDeleted("path2", 1, 3, 0), DeleteRows{2});
  EXPECT_EQ(PositionalDeleteStream(urls, MakeCB(fs_)).GetDeleted("path3", 2, 4, 0), DeleteRows{3});
}

TEST_F(PositionalDeleteTest, MixedStats2) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(fs_->CreateDir(path_));

  ASSERT_OK(WriteFile(fs_, path_ + "/first", Container{{{"path1", 1}, {"path2", 3}}}));
  ASSERT_OK(WriteFile(fs_, path_ + "/second", Container{{{"path1", 2}}}, false));

  std::map<int, std::vector<std::string>> urls = {{0, {path_ + "/first", path_ + "/second"}}};
  EXPECT_EQ(PositionalDeleteStream(urls, MakeCB(fs_)).GetDeleted("path1", 0, 3, 0), DeleteRows({1, 2}));
}

TEST_F(PositionalDeleteTest, ManyQueries) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_OK(fs_->CreateDir(path_));

  ASSERT_OK(WriteFile(fs_, path_ + "/first", Container{{{"path1", 1}, {"path2", 3}}}));
  ASSERT_OK(WriteFile(fs_, path_ + "/second", Container{{{"path1", 2}}}, false));

  std::map<int, std::vector<std::string>> urls = {{0, {path_ + "/first", path_ + "/second"}}};
  auto pos_del_stream = PositionalDeleteStream(urls, MakeCB(fs_));
  EXPECT_EQ(pos_del_stream.GetDeleted("path1", 0, 3, 0), DeleteRows({1, 2}));
  EXPECT_EQ(pos_del_stream.GetDeleted("path2", 0, 2, 0), DeleteRows{});
  EXPECT_EQ(pos_del_stream.GetDeleted("path2", 2, 4, 0), DeleteRows{3});
}

TEST_F(PositionalDeleteTest, MultipleDataFiles) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  ASSERT_EQ(WriteFile(fs_, "delete1.parquet", Container{{{"a", 100}, {"path1", 101}, {"b", 1}}}), Status::OK());
  ASSERT_EQ(WriteFile(fs_, "delete2.parquet", Container{{{"a", 5}, {"a", 6}, {"b", 17}}}), Status::OK());

  std::map<int, std::vector<std::string>> urls = {{0, {"delete1.parquet", "delete2.parquet"}}};
  PositionalDeleteStream stream(urls, MakeCB(fs_));
  ASSERT_EQ(stream.GetDeleted("a", 4, 7, 0), DeleteRows({5, 6}));
}

TEST_F(PositionalDeleteTestLogging, NoExtraBytesRead1) {
  using Container = std::vector<std::vector<DataStringInt64>>;
  const auto path = MakePath("first");
  ASSERT_OK(WriteFile(logging_fs_, path, Container{{{"path1", 1}, {"path2", 3}}}));
  EXPECT_NO_THROW([&]() {
    logging_fs_->Clear();
    auto arrow_reader = OpenUrl(logging_fs_, path).ValueOrDie();
    arrow_reader->num_row_groups();
  }());
  auto metrics = logging_fs_->GetCurrentMetrics();

  EXPECT_NO_THROW([&]() {
    logging_fs_->Clear();
    MakeStream(logging_fs_, path, 0).GetDeleted("path3", 1, 2, 0);
  }());

  EXPECT_EQ(metrics, logging_fs_->GetCurrentMetrics());

  EXPECT_NO_THROW([&]() {
    logging_fs_->Clear();
    MakeStream(logging_fs_, path, 0).GetDeleted("path1", 1, 2, 0);
  }());

  EXPECT_NE(metrics, logging_fs_->GetCurrentMetrics());
}

TEST_F(PositionalDeleteTestLogging, NoExtraBytesRead2) {
  using Container = std::vector<std::vector<DataStringInt64>>;

  const auto first = MakePath("first");
  const auto second = MakePath("second");
  const auto third = MakePath("third");

  ASSERT_OK(WriteFile(logging_fs_, first, Container{{{"path1", 1}}, {{"path5", 5}}, {{"path7", 7}}}));
  ASSERT_OK(WriteFile(logging_fs_, second, Container{{{"path2", 2}}, {{"path3", 3}}, {{"path6", 6}}}));
  ASSERT_OK(WriteFile(logging_fs_, third, Container{{{"path4", 4}}, {{"path8", 8}}, {{"path9", 9}}}));

  logging_fs_->Clear();

  std::map<int, std::vector<std::string>> urls = {{0, {first, second, third}}};
  auto pos_del_stream = PositionalDeleteStream(urls, MakeCB(logging_fs_));
  EXPECT_EQ(pos_del_stream.GetDeleted("path2", 0, INT64_MAX, 0), DeleteRows{2});
  EXPECT_EQ(pos_del_stream.GetDeleted("path3", 0, INT64_MAX, 0), DeleteRows{3});
  EXPECT_EQ(pos_del_stream.GetDeleted("path7", 0, INT64_MAX, 0), DeleteRows{7});
  EXPECT_EQ(pos_del_stream.GetDeleted("path9", 0, INT64_MAX, 0), DeleteRows{9});

  auto metrics = logging_fs_->GetCurrentMetrics();
  logging_fs_->Clear();

  auto Read = [&](const std::string& file_path, const std::vector<int>& indexes) {
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_ASSIGN_OR_RAISE(arrow_reader, OpenUrl(logging_fs_, file_path));

    auto parquet_reader = arrow_reader->parquet_reader();
    for (const auto i : indexes) {
      auto row_group_reader = parquet_reader->RowGroup(i);
      auto column0 = row_group_reader->Column(0);
      auto column1 = row_group_reader->Column(1);
      if (column0->type() != parquet::Type::BYTE_ARRAY) {
        throw arrow::Status::OK();
      }
      if (column1->type() != parquet::Type::INT64) {
        throw arrow::Status::OK();
      }

      auto file_path_reader = std::static_pointer_cast<parquet::ByteArrayReader>(column0);
      auto pos_reader = std::static_pointer_cast<parquet::Int64Reader>(column1);

      parquet::ByteArray file_path_value;
      int64_t pos;
      ReadValueNotNull(file_path_reader, file_path_value);
      ReadValueNotNull(pos_reader, pos);
    }
    return Status::OK();
  };

  ASSERT_OK(Read(first, {2}));
  ASSERT_OK(Read(second, {0, 1}));
  ASSERT_OK(Read(third, {2}));

  EXPECT_EQ(metrics, logging_fs_->GetCurrentMetrics());
}

}  // namespace
}  // namespace iceberg
