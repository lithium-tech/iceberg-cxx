#include "iceberg/streams/iceberg/equality_delete_applier.h"

#include <iceberg/streams/iceberg/mapper.h>
#include <iceberg/streams/iceberg/plan.h>

#include "arrow/status.h"
#include "gtest/gtest.h"
#include "iceberg/common/fs/file_reader_provider_impl.h"
#include "iceberg/common/fs/filesystem_provider_impl.h"
#include "iceberg/equality_delete/handler.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"
#include "iceberg/streams/ut/batch_maker.h"
#include "iceberg/streams/ut/mock_stream.h"
#include "iceberg/tea_scan.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "iceberg/test_utils/write.h"

namespace iceberg {
namespace {

class EqualityDeleteApplierTest : public ::testing::Test {
 public:
  struct CountingLogger : public ILogger {
   public:
    void Log(const Message& msg, const MessageType& message_type) {
      if (message_type == "metrics:equality:files_read") {
        equality_files_read_ += std::stoi(msg);
      }
    }

    int EqualityFilesRead() const { return equality_files_read_; }

   private:
    int equality_files_read_ = 0;
  };

  IcebergBatch MakeSimpleIcebergBatch(std::string column_name, const OptionalVector<int16_t>& values,
                                      PartitionId partition, LayerId layer, std::string path, int row_position) {
    PartitionLayerFilePosition state(PartitionLayerFile(PartitionLayer(partition, layer), path), row_position);
    auto col = MakeInt16ArrowColumn(values);
    auto batch = MakeBatch({col}, {column_name});
    auto iceberg_batch = IcebergBatch(BatchWithSelectionVector(batch, SelectionVector<int32_t>(values.size())), state);

    return iceberg_batch;
  }

  arrow::Result<std::string> PrepareEqualityDeleteFile(const std::vector<ParquetColumn>& columns) {
    std::string del_path =
        "file://" + (dir_.path() / ("del" + std::to_string(written_deletes_++) + ".parquet")).generic_string();
    ARROW_RETURN_NOT_OK(WriteToFile(columns, del_path));

    return del_path;
  }

  std::vector<std::shared_ptr<IcebergBatch>> GetResult(IcebergStreamPtr input_stream, EqualityDeletes info) {
    counting_logger_ = std::make_shared<CountingLogger>();

    auto fs_provider = std::make_shared<FileSystemProvider>(
        std::map<std::string, std::shared_ptr<IFileSystemGetter>>{{"file", std::make_shared<LocalFileSystemGetter>()}});

    std::map<int, std::string> field_id_to_name = {{1, "col1"}, {2, "col2"}};

    EqualityDeleteHandler::Config cfg;
    cfg.use_specialized_deletes = false;
    cfg.equality_delete_max_mb_size = 10;
    cfg.max_rows = 100;
    cfg.throw_if_memory_limit_exceeded = false;

    auto positional_delete_applier =
        std::make_shared<EqualityDeleteApplier>(input_stream, std::make_shared<EqualityDeletes>(std::move(info)), cfg,
                                                std::make_shared<FieldIdMapper>(std::move(field_id_to_name)),
                                                std::make_shared<FileReaderProvider>(fs_provider), counting_logger_);

    std::vector<std::shared_ptr<IcebergBatch>> result_batches;
    while (true) {
      auto batch = positional_delete_applier->ReadNext();
      if (!batch) {
        break;
      }
      result_batches.emplace_back(batch);
    }

    return result_batches;
  }

  void CompareResultWithExpected(const std::vector<std::shared_ptr<arrow::RecordBatch>>& arrow_input_batches,
                                 const std::vector<std::shared_ptr<IcebergBatch>>& result_batches,
                                 const std::vector<SelectionVector<int32_t>>& expected_selection_vectors) {
    ASSERT_EQ(result_batches.size(), arrow_input_batches.size());
    for (size_t i = 0; i < result_batches.size(); ++i) {
      const auto& input_batch = arrow_input_batches[i];
      const auto& result_batch = result_batches[i];

      const auto& input_col = input_batch->GetColumnByName(column_name);
      const auto& result_col = result_batch->GetRecordBatch()->GetColumnByName(column_name);
      EXPECT_TRUE(input_col->Equals(result_col)) << "i = " << i;

      const auto& input_selection_vector = expected_selection_vectors[i];
      const auto& result_selection_vector = result_batch->GetSelectionVector();
      EXPECT_EQ(input_selection_vector.GetVector(), result_selection_vector.GetVector()) << "i = " << i << std::endl;
    }
  }

  int EqualityFilesRead() const { return counting_logger_->EqualityFilesRead(); }

 protected:
  const std::string column_name = "col1";

 private:
  std::shared_ptr<CountingLogger> counting_logger_;

  int written_deletes_ = 0;
  ScopedTempDir dir_;
};

TEST_F(EqualityDeleteApplierTest, Trivial) {
  IcebergStreamPtr input_stream;

  std::string data_path1 = "file://path1";
  std::string data_path2 = "file://path2";

  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_input_batches;
  {
    std::vector<IcebergBatch> input_batches;
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {0, 1, 2, 3, 4}, 0, 0, data_path1, 0));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {10, 11, 12, 13, 14}, 0, 0, data_path1, 10));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {20, 21, 22, 23, 24}, 0, 0, data_path2, 5));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {30, 31, 32, 33, 34}, 0, 0, data_path2, 15));
    for (const auto& input_batch : input_batches) {
      arrow_input_batches.emplace_back(input_batch.GetRecordBatch());
    }

    input_stream = std::make_shared<MockStream<IcebergBatch>>(std::move(input_batches));
  }

  auto col1_data = OptionalVector<int32_t>{2, 4, 12, 20, 30, 31, 32, 33};
  auto column1 = MakeInt16Column("col1", 1, col1_data);
  ASSIGN_OR_FAIL(auto del_path1, PrepareEqualityDeleteFile({column1}));

  EqualityDeletes info;
  info.partlayer_to_deletes[0][0] = {iceberg::ice_tea::EqualityDeleteInfo(del_path1, {1})};

  std::vector<SelectionVector<int32_t>> expected_selection_vectors = {
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 3}),
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 3, 4}),
      SelectionVector<int32_t>(std::vector<int32_t>{1, 2, 3, 4}),
      SelectionVector<int32_t>(std::vector<int32_t>{4}),
  };

  std::vector<std::shared_ptr<IcebergBatch>> result_batches = GetResult(input_stream, std::move(info));

  CompareResultWithExpected(arrow_input_batches, result_batches, expected_selection_vectors);

  EXPECT_EQ(EqualityFilesRead(), 1);
}

TEST_F(EqualityDeleteApplierTest, ManyPartitions) {
  IcebergStreamPtr input_stream;

  std::string data_path1 = "file://path1";
  std::string data_path2 = "file://path2";

  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_input_batches;
  {
    std::vector<IcebergBatch> input_batches;
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {0, 1, 2, 3, 4}, 1, 0, data_path1, 0));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {10, 11, 12, 13, 14}, 0, 0, data_path1, 10));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {20, 21, 22, 23, 24}, 2, 0, data_path2, 5));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {30, 31, 32, 33, 34}, 3, 0, data_path2, 15));
    for (const auto& input_batch : input_batches) {
      arrow_input_batches.emplace_back(input_batch.GetRecordBatch());
    }

    input_stream = std::make_shared<MockStream<IcebergBatch>>(std::move(input_batches));
  }

  auto col1_data = OptionalVector<int32_t>{2, 4, 12, 20, 30, 31, 32, 33};
  auto column1 = MakeInt16Column("col1", 1, col1_data);
  ASSIGN_OR_FAIL(auto del_path1, PrepareEqualityDeleteFile({column1}));

  EqualityDeletes info;
  info.partlayer_to_deletes[0][0] = {iceberg::ice_tea::EqualityDeleteInfo(del_path1, {1})};

  std::vector<SelectionVector<int32_t>> expected_selection_vectors = {
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 2, 3, 4}),
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 3, 4}),
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 2, 3, 4}),
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 2, 3, 4}),
  };

  std::vector<std::shared_ptr<IcebergBatch>> result_batches = GetResult(input_stream, std::move(info));

  CompareResultWithExpected(arrow_input_batches, result_batches, expected_selection_vectors);

  EXPECT_EQ(EqualityFilesRead(), 1);
}

TEST_F(EqualityDeleteApplierTest, ManyLayers) {
  IcebergStreamPtr input_stream;

  std::string data_path1 = "file://path1";
  std::string data_path2 = "file://path2";
  std::string data_path3 = "file://path3";
  std::string data_path4 = "file://path4";

  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_input_batches;
  {
    std::vector<IcebergBatch> input_batches;
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {0, 1, 2, 3, 4}, 0, 1, data_path1, 0));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {0, 1, 2, 3, 4}, 0, 4, data_path2, 10));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {0, 1, 2, 3, 4}, 0, 2, data_path3, 5));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {0, 1, 2, 3, 4}, 0, 3, data_path4, 15));
    for (const auto& input_batch : input_batches) {
      arrow_input_batches.emplace_back(input_batch.GetRecordBatch());
    }

    input_stream = std::make_shared<MockStream<IcebergBatch>>(std::move(input_batches));
  }

  std::string delete_path1;
  {
    auto col1_data = OptionalVector<int32_t>{2};
    auto column1 = MakeInt16Column("col1", 1, col1_data);
    ASSIGN_OR_FAIL(delete_path1, PrepareEqualityDeleteFile({column1}));
  }
  std::string delete_path2;
  {
    auto col1_data = OptionalVector<int32_t>{0};
    auto column1 = MakeInt16Column("col1", 1, col1_data);
    ASSIGN_OR_FAIL(delete_path2, PrepareEqualityDeleteFile({column1}));
  }
  std::string delete_path3;
  {
    auto col1_data = OptionalVector<int32_t>{4};
    auto column1 = MakeInt16Column("col1", 1, col1_data);
    ASSIGN_OR_FAIL(delete_path3, PrepareEqualityDeleteFile({column1}));
  }

  EqualityDeletes info;
  info.partlayer_to_deletes[0][1] = {
      iceberg::ice_tea::EqualityDeleteInfo{delete_path1, {1}}};  // applied to layers <= 1
  info.partlayer_to_deletes[0][3] = {
      iceberg::ice_tea::EqualityDeleteInfo{delete_path2, {1}}};  // applied to layers <= 3
  info.partlayer_to_deletes[0][1'000'000'000] = {
      iceberg::ice_tea::EqualityDeleteInfo{delete_path3, {1}}};  // applied to layers <= 1'000'000'000

  std::vector<SelectionVector<int32_t>> expected_selection_vectors = {
      SelectionVector<int32_t>(std::vector<int32_t>{1, 3}),        // layer 1
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 2, 3}),  // layer 4
      SelectionVector<int32_t>(std::vector<int32_t>{1, 2, 3}),     // layer 2
      SelectionVector<int32_t>(std::vector<int32_t>{1, 2, 3}),     // layer 3
  };

  std::vector<std::shared_ptr<IcebergBatch>> result_batches = GetResult(input_stream, std::move(info));

  CompareResultWithExpected(arrow_input_batches, result_batches, expected_selection_vectors);

  EXPECT_EQ(EqualityFilesRead(), 3);
}

TEST(EqualityDelete, GetFieldIds) {
  std::map<PartitionId, std::map<LayerId, std::vector<iceberg::ice_tea::EqualityDeleteInfo>>> partlayer_to_deletes;

  partlayer_to_deletes[0][1] = {iceberg::ice_tea::EqualityDeleteInfo("a", {1})};
  partlayer_to_deletes[0][2] = {iceberg::ice_tea::EqualityDeleteInfo("b", {2})};
  partlayer_to_deletes[0][3] = {iceberg::ice_tea::EqualityDeleteInfo("c", {3})};

  partlayer_to_deletes[1][1] = {iceberg::ice_tea::EqualityDeleteInfo("d", {1, 2})};
  partlayer_to_deletes[1][2] = {iceberg::ice_tea::EqualityDeleteInfo("e", {2, 3})};
  partlayer_to_deletes[1][3] = {iceberg::ice_tea::EqualityDeleteInfo("f", {3, 4})};

  EqualityDeletes deletes{.partlayer_to_deletes = std::move(partlayer_to_deletes)};

  EXPECT_EQ(deletes.GetFieldIds(PartitionLayer(0, 0)), (std::vector<int32_t>{1, 2, 3}));
  EXPECT_EQ(deletes.GetFieldIds(PartitionLayer(0, 1)), (std::vector<int32_t>{1, 2, 3}));
  EXPECT_EQ(deletes.GetFieldIds(PartitionLayer(0, 2)), (std::vector<int32_t>{2, 3}));
  EXPECT_EQ(deletes.GetFieldIds(PartitionLayer(0, 3)), (std::vector<int32_t>{3}));
  EXPECT_EQ(deletes.GetFieldIds(PartitionLayer(0, 4)), (std::vector<int32_t>{}));

  EXPECT_EQ(deletes.GetFieldIds(PartitionLayer(1, 0)), (std::vector<int32_t>{1, 2, 3, 4}));
  EXPECT_EQ(deletes.GetFieldIds(PartitionLayer(1, 1)), (std::vector<int32_t>{1, 2, 3, 4}));
  EXPECT_EQ(deletes.GetFieldIds(PartitionLayer(1, 2)), (std::vector<int32_t>{2, 3, 4}));
  EXPECT_EQ(deletes.GetFieldIds(PartitionLayer(1, 3)), (std::vector<int32_t>{3, 4}));
  EXPECT_EQ(deletes.GetFieldIds(PartitionLayer(1, 4)), (std::vector<int32_t>{}));
}

}  // namespace
}  // namespace iceberg
