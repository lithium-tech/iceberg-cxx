#include "iceberg/streams/iceberg/equality_delete_applier.h"

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

TEST(EqualityDeleteApplier, Trivial) {
  IcebergStreamPtr input_stream;

  std::string data_path1 = "file://path1";
  std::string data_path2 = "file://path2";

  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_input_batches;
  {
    PartitionLayerFilePosition state1(PartitionLayerFile(PartitionLayer(0, 0), data_path1), 0);
    auto col1 = MakeInt16ArrowColumn({0, 1, 2, 3, 4});
    auto batch1 = MakeBatch({col1}, {"col1"});
    auto iceberg_batch1 = IcebergBatch(BatchWithSelectionVector(batch1, SelectionVector<int32_t>(5)), state1);

    PartitionLayerFilePosition state2(PartitionLayerFile(PartitionLayer(0, 0), data_path1), 10);
    auto col2 = MakeInt16ArrowColumn({10, 11, 12, 13, 14});
    auto batch2 = MakeBatch({col2}, {"col1"});
    auto iceberg_batch2 = IcebergBatch(BatchWithSelectionVector(batch2, SelectionVector<int32_t>(5)), state2);

    PartitionLayerFilePosition state3(PartitionLayerFile(PartitionLayer(0, 0), data_path2), 5);
    auto col3 = MakeInt16ArrowColumn({20, 21, 22, 23, 24});
    auto batch3 = MakeBatch({col3}, {"col1"});
    auto iceberg_batch3 = IcebergBatch(BatchWithSelectionVector(batch3, SelectionVector<int32_t>(5)), state3);

    PartitionLayerFilePosition state4(PartitionLayerFile(PartitionLayer(0, 0), data_path2), 15);
    auto col4 = MakeInt16ArrowColumn({30, 31, 32, 33, 34});
    auto batch4 = MakeBatch({col4}, {"col1"});
    auto iceberg_batch4 = IcebergBatch(BatchWithSelectionVector(batch4, SelectionVector<int32_t>(5)), state4);

    std::vector<IcebergBatch> input_batches;
    input_batches.emplace_back(std::move(iceberg_batch1));
    input_batches.emplace_back(std::move(iceberg_batch2));
    input_batches.emplace_back(std::move(iceberg_batch3));
    input_batches.emplace_back(std::move(iceberg_batch4));

    for (const auto& input_batch : input_batches) {
      arrow_input_batches.emplace_back(input_batch.GetRecordBatch());
    }

    input_stream = std::make_shared<MockStream<IcebergBatch>>(std::move(input_batches));
  }

  ScopedTempDir dir;
  std::string del_path1 = "file://" + (dir.path() / "del1.parquet").generic_string();

  std::vector<SelectionVector<int32_t>> expected_selection_vectors = {
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 3}),
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 3, 4}),
      SelectionVector<int32_t>(std::vector<int32_t>{1, 2, 3, 4}),
      SelectionVector<int32_t>(std::vector<int32_t>{4}),
  };

  auto col1_data = OptionalVector<int32_t>{2, 4, 12, 20, 30, 31, 32, 33};

  auto column1 = MakeInt16Column("col1", 2, col1_data);
  ASSERT_OK(WriteToFile({column1}, del_path1));

  try {
    EqualityDeletes info;
    info.partlayer_to_deletes[0][0] = {iceberg::ice_tea::EqualityDeleteInfo(del_path1, {2})};
    auto mapper = std::make_shared<FieldIdMapper>(std::map<int, std::string>{{2, "col1"}});
    auto fs_provider = std::make_shared<FileSystemProvider>(
        std::map<std::string, std::shared_ptr<IFileSystemGetter>>{{"file", std::make_shared<LocalFileSystemGetter>()}});

    EqualityDeleteHandler::Config cfg;
    cfg.use_specialized_deletes = false;
    cfg.equality_delete_max_mb_size = 10;
    cfg.max_rows = 100;
    cfg.throw_if_memory_limit_exceeded = false;

    auto equality_delete_applier =
        std::make_shared<EqualityDeleteApplier>(input_stream, std::make_shared<EqualityDeletes>(std::move(info)), cfg,
                                                mapper, std::make_shared<FileReaderProvider>(fs_provider));

    std::vector<std::shared_ptr<IcebergBatch>> result_batches;
    while (true) {
      auto batch = equality_delete_applier->ReadNext();
      if (!batch) {
        break;
      }
      result_batches.emplace_back(batch);
    }

    ASSERT_EQ(result_batches.size(), arrow_input_batches.size());
    for (size_t i = 0; i < result_batches.size(); ++i) {
      const std::string column_name = "col1";
      const auto& input_batch = arrow_input_batches[i];
      const auto& result_batch = result_batches[i];

      const auto& input_col = input_batch->GetColumnByName(column_name);
      const auto& result_col = result_batch->GetRecordBatch()->GetColumnByName(column_name);
      EXPECT_TRUE(input_col->Equals(result_col)) << "i = " << i;

      const auto& input_selection_vector = expected_selection_vectors[i];
      const auto& result_selection_vector = result_batch->GetSelectionVector();
      EXPECT_EQ(input_selection_vector.GetVector(), result_selection_vector.GetVector()) << "i = " << i << std::endl;
    }
  } catch (arrow::Status& s) {
    std::cerr << s.ToString() << std::endl;
  }
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
