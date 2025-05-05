#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array/builder_primitive.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "gtest/gtest.h"
#include "iceberg/equality_delete/common.h"
#include "iceberg/equality_delete/handler.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "iceberg/test_utils/write.h"
#include "parquet/arrow/reader.h"

namespace iceberg {
namespace {
using arrow::fs::FileSystem;
using arrow::fs::LocalFileSystem;

class EqualityDeleteHandlerTest : public ::testing::Test {
 protected:
  struct DataFragmentInfo {
    std::string url;
    int64_t offset;
    int64_t size;
  };

  void SetUp() override { fs_ = std::make_shared<arrow::fs::LocalFileSystem>(); }

  EqualityDeleteHandler MakeHandler(std::optional<uint64_t> rows_limit, bool use_specialized_deletes,
                                    std::optional<uint64_t> mb_size_limit, bool throw_on_limit_exceeded) {
    EqualityDeleteHandler::Config config;
    config.max_rows = rows_limit.value_or(std::numeric_limits<uint64_t>::max());
    config.use_specialized_deletes = use_specialized_deletes;
    config.equality_delete_max_mb_size = mb_size_limit.value_or(std::numeric_limits<int32_t>::max());
    config.throw_if_memory_limit_exceeded = throw_on_limit_exceeded;
    return EqualityDeleteHandler(
        [&](const std::string& url) -> arrow::Result<std::unique_ptr<parquet::arrow::FileReader>> {
          auto path = std::string(url.substr(url.find("://") + 3));
          file_open_calls++;
          ARROW_ASSIGN_OR_RAISE(auto input_file, fs_->OpenInputFile(path));

          parquet::arrow::FileReaderBuilder reader_builder;
          ARROW_RETURN_NOT_OK(reader_builder.Open(input_file));
          reader_builder.memory_pool(arrow::default_memory_pool());

          std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
          ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());

          return arrow_reader;
        },
        config);
  }

  std::string GetFileUrl(const std::string& file_name) const {
    return "file://" + (dir_.path() / file_name).generic_string();
  }

  size_t file_open_calls = 0;
  ScopedTempDir dir_;
  std::shared_ptr<FileSystem> fs_;
};

static std::shared_ptr<arrow::Array> CreateInt32Array(const OptionalVector<int>& values) {
  arrow::Int32Builder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      if (auto status = builder.Append(value.value()); !status.ok()) {
        throw status;
      }
    } else {
      if (auto status = builder.AppendNull(); !status.ok()) {
        throw status;
      }
    }
  }
  auto maybe_array = builder.Finish();
  if (!maybe_array.ok()) {
    throw maybe_array.status();
  }
  return maybe_array.ValueUnsafe();
}

TEST_F(EqualityDeleteHandlerTest, SanityCheck) {
  std::string del_f1_path = GetFileUrl("del_f1.parquet");
  auto column = MakeInt32Column("f1", 1, OptionalVector<int32_t>{2, 3});
  ASSERT_OK(WriteToFile({column}, del_f1_path));

  EqualityDeleteHandler handler = MakeHandler(std::nullopt, false, std::nullopt, true);

  ASSERT_OK(handler.AppendDelete(del_f1_path, {1}));

  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(handler.PrepareDeletesForFile());
    std::shared_ptr<arrow::Array> array = CreateInt32Array({1, 2, 3, 4});
    handler.PrepareDeletesForBatch(std::map<FieldId, std::shared_ptr<arrow::Array>>{{1, array}});
    EXPECT_EQ(handler.IsDeleted(0), false);
    EXPECT_EQ(handler.IsDeleted(1), true);
    EXPECT_EQ(handler.IsDeleted(2), true);
    EXPECT_EQ(handler.IsDeleted(3), false);
  }
}

TEST_F(EqualityDeleteHandlerTest, NullInDelete) {
  std::string del_f1_path = GetFileUrl("del_f1.parquet");
  auto column = MakeInt32Column("f1", 1, OptionalVector<int32_t>{std::nullopt, 3});
  ASSERT_OK(WriteToFile({column}, del_f1_path));

  EqualityDeleteHandler handler = MakeHandler(std::nullopt, false, std::nullopt, true);

  ASSERT_OK(handler.AppendDelete(del_f1_path, {1}));

  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(handler.PrepareDeletesForFile());
    std::shared_ptr<arrow::Array> array = CreateInt32Array({1, std::nullopt, 3, 4});
    handler.PrepareDeletesForBatch(std::map<FieldId, std::shared_ptr<arrow::Array>>{{1, array}});
    EXPECT_EQ(handler.IsDeleted(0), false);
    EXPECT_EQ(handler.IsDeleted(1), true);
    EXPECT_EQ(handler.IsDeleted(2), true);
    EXPECT_EQ(handler.IsDeleted(3), false);
  }
}

TEST_F(EqualityDeleteHandlerTest, NullInDeleteMultipleColumns) {
  std::string del_f1_path = GetFileUrl("del_f1.parquet");
  auto column1 = MakeInt32Column("f1", 1, OptionalVector<int32_t>{std::nullopt, 3, 2, std::nullopt});
  auto column2 = MakeInt32Column("f2", 2, OptionalVector<int32_t>{2, 3, std::nullopt, std::nullopt});
  ASSERT_OK(WriteToFile({column1, column2}, del_f1_path));

  EqualityDeleteHandler handler = MakeHandler(std::nullopt, false, std::nullopt, true);

  ASSERT_OK(handler.AppendDelete(del_f1_path, {1, 2}));

  ASSERT_TRUE(handler.PrepareDeletesForFile());
  std::shared_ptr<arrow::Array> array1 = CreateInt32Array({std::nullopt, 3, 2, std::nullopt, std::nullopt, 2, 3});
  std::shared_ptr<arrow::Array> array2 = CreateInt32Array({2, 3, std::nullopt, std::nullopt, 3, 2, std::nullopt});
  handler.PrepareDeletesForBatch(std::map<FieldId, std::shared_ptr<arrow::Array>>{{1, array1}, {2, array2}});
  EXPECT_EQ(handler.IsDeleted(0), true);
  EXPECT_EQ(handler.IsDeleted(1), true);
  EXPECT_EQ(handler.IsDeleted(2), true);
  EXPECT_EQ(handler.IsDeleted(3), true);
  EXPECT_EQ(handler.IsDeleted(4), false);
  EXPECT_EQ(handler.IsDeleted(5), false);
  EXPECT_EQ(handler.IsDeleted(6), false);
}

TEST_F(EqualityDeleteHandlerTest, OneFragmentMultipleDeletes) {
  std::string del1_path = GetFileUrl("del1.parquet");
  auto column1 = MakeInt32Column("f1", 1, OptionalVector<int32_t>{2, 3});
  auto column2 = MakeInt32Column("f2", 2, OptionalVector<int32_t>{5, 6});
  ASSERT_OK(WriteToFile({column1, column2}, del1_path));

  std::string del2_path = GetFileUrl("del2.parquet");
  auto column3 = MakeInt32Column("f2", 2, OptionalVector<int32_t>{1, 4});
  auto column4 = MakeInt32Column("f3", 3, OptionalVector<int32_t>{7, 8});
  ASSERT_OK(WriteToFile({column3, column4}, del2_path));

  EqualityDeleteHandler handler = MakeHandler(std::nullopt, false, std::nullopt, true);

  ASSERT_OK(handler.AppendDelete(del1_path, {1, 2}));
  ASSERT_OK(handler.AppendDelete(del2_path, {2, 3}));

  ASSERT_TRUE(handler.PrepareDeletesForFile());
  std::shared_ptr<arrow::Array> array1 = CreateInt32Array({2, 3, 0, 0, 0});
  std::shared_ptr<arrow::Array> array2 = CreateInt32Array({5, 6, 1, 4, 0});
  std::shared_ptr<arrow::Array> array3 = CreateInt32Array({0, 0, 7, 8, 0});
  handler.PrepareDeletesForBatch(
      std::map<FieldId, std::shared_ptr<arrow::Array>>{{1, array1}, {2, array2}, {3, array3}});
  EXPECT_EQ(handler.IsDeleted(0), true);
  EXPECT_EQ(handler.IsDeleted(1), true);
  EXPECT_EQ(handler.IsDeleted(2), true);
  EXPECT_EQ(handler.IsDeleted(3), true);
  EXPECT_EQ(handler.IsDeleted(4), false);
}

TEST_F(EqualityDeleteHandlerTest, RowLimitExceeded) {
  std::string del1_path = GetFileUrl("del1.parquet");
  auto column1 = MakeInt32Column("f1", 1, OptionalVector<int32_t>{2, 3});
  ASSERT_OK(WriteToFile({column1}, del1_path));

  for (int lim : {0, 1}) {
    EqualityDeleteHandler handler = MakeHandler(lim, false, std::nullopt, true);
    ASSERT_NE(handler.AppendDelete(del1_path, {1}), arrow::Status::OK());
  }
}

TEST_F(EqualityDeleteHandlerTest, MultipleFiles) {
  std::string del1_path = GetFileUrl("del1.parquet");
  auto column1 = MakeInt32Column("f1", 1, OptionalVector<int32_t>{2, 3, 4, 5});
  ASSERT_OK(WriteToFile({column1}, del1_path));

  std::string del2_path = GetFileUrl("del2.parquet");
  auto column2 = MakeInt32Column("f1", 1, OptionalVector<int32_t>{6, 7, 8, 9});
  ASSERT_OK(WriteToFile({column2}, del2_path));

  EqualityDeleteHandler handler = MakeHandler(6, false, std::nullopt, true);
  ASSERT_EQ(handler.AppendDelete(del1_path, {1}), arrow::Status::OK());

  auto status = handler.AppendDelete(del2_path, {1});
  ASSERT_NE(status, arrow::Status::OK());

  auto message = status.message();
  EXPECT_EQ(message, "Equality delete rows limit exceeded (8/6)");
}

TEST_F(EqualityDeleteHandlerTest, MBSizeLimitExceeded) {
  std::string del1_path = GetFileUrl("del1.parquet");
  auto column1 = MakeInt32Column("f1", 1, OptionalVector<int32_t>{2, 3});
  ASSERT_OK(WriteToFile({column1}, del1_path));

  for (int lim : {0}) {
    EqualityDeleteHandler handler = MakeHandler(std::nullopt, false, lim, true);
    EXPECT_ANY_THROW((void)handler.AppendDelete(del1_path, {1}));
  }
}

TEST_F(EqualityDeleteHandlerTest, Grouping) {
  std::string del1_path = GetFileUrl("del1.parquet");
  auto column2 = MakeInt32Column("f1", 1, OptionalVector<int32_t>{2});
  ASSERT_OK(WriteToFile({column2}, del1_path));

  std::string del2_path = GetFileUrl("del2.parquet");
  auto column3 = MakeInt32Column("f1", 1, OptionalVector<int32_t>{3});
  ASSERT_OK(WriteToFile({column3}, del2_path));

  EqualityDeleteHandler handler = MakeHandler(std::nullopt, false, std::nullopt, true);
  ASSERT_OK(handler.AppendDelete(del1_path, {1}));
  ASSERT_OK(handler.AppendDelete(del2_path, {1}));

  handler.PrepareDeletesForFile();

  std::shared_ptr<arrow::Array> array = CreateInt32Array({1, 2, 3, 4});
  handler.PrepareDeletesForBatch(std::map<FieldId, std::shared_ptr<arrow::Array>>{{1, array}});
  EXPECT_EQ(handler.IsDeleted(0), false);
  EXPECT_EQ(handler.IsDeleted(1), true);
  EXPECT_EQ(handler.IsDeleted(2), true);
  EXPECT_EQ(handler.IsDeleted(3), false);
}

TEST_F(EqualityDeleteHandlerTest, Empty) {
  std::string del_f1_path = GetFileUrl("del_f1.parquet");
  auto column2 = MakeInt32Column("f1", 1, OptionalVector<int32_t>{});
  ASSERT_OK(WriteToFile({column2}, del_f1_path));

  EqualityDeleteHandler handler = MakeHandler(std::nullopt, false, std::nullopt, true);
  ASSERT_OK(handler.AppendDelete(del_f1_path, {1}));

  ASSERT_FALSE(handler.PrepareDeletesForFile());
}

}  // namespace
}  // namespace iceberg
