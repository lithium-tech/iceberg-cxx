#include "iceberg/streams/iceberg/positional_delete_applier.h"

#include <arrow/record_batch.h>
#include <iceberg/tea_scan.h>

#include "arrow/status.h"
#include "gtest/gtest.h"
#include "iceberg/common/fs/file_reader_provider_impl.h"
#include "iceberg/common/fs/filesystem_provider_impl.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"
#include "iceberg/streams/iceberg/plan.h"
#include "iceberg/streams/ut/batch_maker.h"
#include "iceberg/streams/ut/mock_stream.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "iceberg/test_utils/write.h"

namespace iceberg {
namespace {

class PositionalDeleteApplierTest : public ::testing::Test {
 public:
  IcebergBatch MakeSimpleIcebergBatch(std::string column_name, const OptionalVector<int16_t>& values,
                                      PartitionId partition, LayerId layer, std::string path, int row_position) {
    PartitionLayerFilePosition state(PartitionLayerFile(PartitionLayer(partition, layer), path), row_position);
    auto col = MakeInt16ArrowColumn(values);
    auto batch = MakeBatch({col}, {column_name});
    auto iceberg_batch = IcebergBatch(BatchWithSelectionVector(batch, SelectionVector<int32_t>(values.size())), state);

    return iceberg_batch;
  }

  arrow::Result<std::string> PreparePositionalDeleteFile(std::vector<std::string*> filenames,
                                                         OptionalVector<int64_t> row_positions) {
    auto column1 = MakeStringColumn("file_path", 1, filenames);
    auto column2 = MakeInt64Column("pos", 2, row_positions);

    std::string del_path =
        "file://" + (dir_.path() / ("del" + std::to_string(written_deletes_++) + ".parquet")).generic_string();
    ARROW_RETURN_NOT_OK(WriteToFile({column1, column2}, del_path));

    return del_path;
  }

  std::vector<std::shared_ptr<IcebergBatch>> GetResult(IcebergStreamPtr input_stream, PositionalDeletes info) {
    auto fs_provider = std::make_shared<FileSystemProvider>(
        std::map<std::string, std::shared_ptr<IFileSystemGetter>>{{"file", std::make_shared<LocalFileSystemGetter>()}});
    auto positional_delete_applier = std::make_shared<PositionalDeleteApplier>(
        input_stream, std::move(info), std::make_shared<FileReaderProvider>(fs_provider));

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

 private:
  int written_deletes_ = 0;
  ScopedTempDir dir_;
};

TEST_F(PositionalDeleteApplierTest, OnePartitionOneLayerSortedOrder) {
  IcebergStreamPtr input_stream;

  std::string data_path1 = "file://path1";
  std::string data_path2 = "file://path2";

  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_input_batches;
  {
    std::vector<IcebergBatch> input_batches;
    input_batches.emplace_back(MakeSimpleIcebergBatch("col1", {0, 1, 2, 3, 4}, 0, 0, data_path1, 0));
    input_batches.emplace_back(MakeSimpleIcebergBatch("col2", {10, 11, 12, 13, 14}, 0, 0, data_path1, 10));
    input_batches.emplace_back(MakeSimpleIcebergBatch("col3", {20, 21, 22, 23, 24}, 0, 0, data_path2, 5));
    input_batches.emplace_back(MakeSimpleIcebergBatch("col4", {30, 31, 32, 33, 34}, 0, 0, data_path2, 15));

    for (const auto& input_batch : input_batches) {
      arrow_input_batches.emplace_back(input_batch.GetRecordBatch());
    }

    input_stream = std::make_shared<MockStream<IcebergBatch>>(std::move(input_batches));
  }

  std::vector<SelectionVector<int32_t>> expected_selection_vectors = {
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 3}),
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 3, 4}),
      SelectionVector<int32_t>(std::vector<int32_t>{1, 2, 3, 4}),
      SelectionVector<int32_t>(std::vector<int32_t>{4}),
  };

  auto col1_data = std::vector<std::string*>{&data_path1, &data_path1, &data_path1, &data_path1, &data_path1,
                                             &data_path2, &data_path2, &data_path2, &data_path2, &data_path2,
                                             &data_path2, &data_path2, &data_path2};
  auto col2_data = OptionalVector<int64_t>{2, 4, 7, 12, 15, 3, 5, 10, 15, 16, 17, 18, 120};

  ASSIGN_OR_FAIL(auto delete_path, PreparePositionalDeleteFile(col1_data, col2_data));

  PositionalDeletes info;
  info.delete_entries[0][0] = {iceberg::ice_tea::PositionalDeleteInfo{delete_path}};
  std::vector<std::shared_ptr<IcebergBatch>> result_batches = GetResult(std::move(input_stream), std::move(info));

  ASSERT_EQ(result_batches.size(), arrow_input_batches.size());
  for (size_t i = 0; i < result_batches.size(); ++i) {
    const std::string column_name = "col" + std::to_string(i + 1);
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

}  // namespace
}  // namespace iceberg
