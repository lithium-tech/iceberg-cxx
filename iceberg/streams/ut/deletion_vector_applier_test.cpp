#include "iceberg/streams/iceberg/deletion_vector_applier.h"

#include <arrow/record_batch.h>
#include <iceberg/common/logger.h>
#include <iceberg/tea_scan.h>

#include <fstream>

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
#include "iceberg/deletion_vector.h"
#include "iceberg/puffin.h"

namespace iceberg {
namespace {

class DeletionVectorApplierTest : public ::testing::Test {
 public:
  struct CountingLogger : public ILogger {
   public:
    void Log(const Message& msg, const MessageType& message_type) {
      if (message_type == "metrics:deletion_vector:deleted_rows") {
        deleted_rows_count_ += std::stoi(msg);
      }
    }

    int DeletedRowsCount() const { return deleted_rows_count_; }

   private:
    int deleted_rows_count_ = 0;
  };

  IcebergBatch MakeSimpleIcebergBatch(std::string column_name, const OptionalVector<int16_t>& values,
                                      PartitionId partition, LayerId layer, std::string path, int row_position) {
    PartitionLayerFilePosition state(PartitionLayerFile(PartitionLayer(partition, layer), path), row_position);
    auto col = MakeInt16ArrowColumn(values);
    auto batch = MakeBatch({col}, {column_name});
    auto iceberg_batch = IcebergBatch(BatchWithSelectionVector(batch, SelectionVector<int32_t>(values.size())), state);

    return iceberg_batch;
  }

  struct DVFileInfo {
    std::string path;
    int64_t offset;
    int64_t length;
  };

  arrow::Result<DVFileInfo> PrepareDeletionVectorFile(const std::vector<uint64_t>& rows_to_delete,
                                                         const std::string& referenced_data_file) {
    // Create DeletionVector
    PuffinFile::Footer::BlobMetadata dummy_meta;
    dummy_meta.type = DeletionVector::kBlobType;
    dummy_meta.properties[std::string(properties_names::kReferencedDataFile)] = referenced_data_file;
    dummy_meta.properties[std::string(properties_names::kCardinality)] = std::to_string(0); // placeholder
    DeletionVector dv(dummy_meta, roaring::Roaring64Map());
    dv.AddMany(rows_to_delete);
    
    std::string blob_data = dv.GetBlob();
    auto properties = dv.GetProperties();

    PuffinFileBuilder builder;
    builder.SetSnapshotId(1);
    builder.SetSequenceNumber(1);
    builder.AppendBlob(blob_data, properties, {}, std::string(DeletionVector::kBlobType));

    PuffinFile puffin_file = std::move(builder).Build();
    std::string payload = puffin_file.GetPayload();

    std::string filename = (dir_.path() / ("dv" + std::to_string(written_dvs_++) + ".puffin")).generic_string();
    std::ofstream out(filename, std::ios::binary);
    out.write(payload.data(), payload.size());
    out.close();

    // Get offset and length from the footer of the created file
    auto footer = puffin_file.GetFooter().GetDeserializedFooter();
    auto blob_meta = footer.blobs[0];

    return DVFileInfo{"file://" + filename, blob_meta.offset, blob_meta.length};
  }

  std::vector<std::shared_ptr<IcebergBatch>> GetResult(IcebergStreamPtr input_stream, DeletionVectors info) {
    counting_logger_ = std::make_shared<CountingLogger>();

    auto fs_provider = std::make_shared<FileSystemProvider>(
        std::map<std::string, std::shared_ptr<IFileSystemGetter>>{{"file", std::make_shared<LocalFileSystemGetter>()}});
    auto dv_applier = std::make_shared<DeletionVectorApplier>(
        input_stream, std::move(info), std::make_shared<FileReaderProvider>(fs_provider), counting_logger_);

    std::vector<std::shared_ptr<IcebergBatch>> result_batches;
    while (true) {
      auto batch = dv_applier->ReadNext();
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
      const auto& result_batch = result_batches[i];

      const auto& input_selection_vector = expected_selection_vectors[i];
      const auto& result_selection_vector = result_batch->GetSelectionVector();
      EXPECT_EQ(input_selection_vector.GetVector(), result_selection_vector.GetVector()) << "i = " << i << std::endl;
    }
  }

 protected:
  const std::string column_name = "col1";
  std::shared_ptr<CountingLogger> counting_logger_;

 private:
  int written_dvs_ = 0;
  ScopedTempDir dir_;
};

TEST_F(DeletionVectorApplierTest, SimpleCase) {
  IcebergStreamPtr input_stream;

  std::string data_path = "file://data.parquet";

  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_input_batches;
  {
    std::vector<IcebergBatch> input_batches;
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {0, 1, 2, 3, 4}, 0, 0, data_path, 0));

    for (const auto& input_batch : input_batches) {
      arrow_input_batches.emplace_back(input_batch.GetRecordBatch());
    }

    input_stream = std::make_shared<MockStream<IcebergBatch>>(std::move(input_batches));
  }

  // Delete rows 1 and 3 (0-based)
  ASSIGN_OR_FAIL(auto dv_file, PrepareDeletionVectorFile({1, 3}, data_path));

  DeletionVectors info;
  info.dv_entries[0][0] = {iceberg::ice_tea::DeletionVectorInfo{dv_file.path, dv_file.offset, dv_file.length, data_path}};

  std::vector<SelectionVector<int32_t>> expected_selection_vectors = {
      SelectionVector<int32_t>(std::vector<int32_t>{0, 2, 4}),
  };

  std::vector<std::shared_ptr<IcebergBatch>> result_batches = GetResult(std::move(input_stream), std::move(info));

  CompareResultWithExpected(arrow_input_batches, result_batches, expected_selection_vectors);
  EXPECT_EQ(counting_logger_->DeletedRowsCount(), 2);
}

TEST_F(DeletionVectorApplierTest, AppliedOnlyToReferencedFile) {
  IcebergStreamPtr input_stream;

  std::string data_path1 = "file://data1.parquet";
  std::string data_path2 = "file://data2.parquet";

  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_input_batches;
  {
    std::vector<IcebergBatch> input_batches;
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {0, 1, 2}, 0, 0, data_path1, 0));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {0, 1, 2}, 0, 0, data_path2, 0));

    for (const auto& input_batch : input_batches) {
      arrow_input_batches.emplace_back(input_batch.GetRecordBatch());
    }

    input_stream = std::make_shared<MockStream<IcebergBatch>>(std::move(input_batches));
  }

  // Delete row 1 in data1
  ASSIGN_OR_FAIL(auto dv_file, PrepareDeletionVectorFile({1}, data_path1));

  DeletionVectors info;
  info.dv_entries[0][0] = {iceberg::ice_tea::DeletionVectorInfo{dv_file.path, dv_file.offset, dv_file.length, data_path1}};

  std::vector<SelectionVector<int32_t>> expected_selection_vectors = {
      SelectionVector<int32_t>(std::vector<int32_t>{0, 2}),
      SelectionVector<int32_t>(std::vector<int32_t>{0, 1, 2}),
  };

  std::vector<std::shared_ptr<IcebergBatch>> result_batches = GetResult(std::move(input_stream), std::move(info));

  CompareResultWithExpected(arrow_input_batches, result_batches, expected_selection_vectors);
  EXPECT_EQ(counting_logger_->DeletedRowsCount(), 1);
}

TEST_F(DeletionVectorApplierTest, MultipleBatches) {
  IcebergStreamPtr input_stream;

  std::string data_path = "file://data.parquet";

  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_input_batches;
  {
    std::vector<IcebergBatch> input_batches;
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {0, 1, 2}, 0, 0, data_path, 0));
    input_batches.emplace_back(MakeSimpleIcebergBatch(column_name, {3, 4, 5}, 0, 0, data_path, 3));

    for (const auto& input_batch : input_batches) {
      arrow_input_batches.emplace_back(input_batch.GetRecordBatch());
    }

    input_stream = std::make_shared<MockStream<IcebergBatch>>(std::move(input_batches));
  }

  // Delete rows 1 and 4
  ASSIGN_OR_FAIL(auto dv_file, PrepareDeletionVectorFile({1, 4}, data_path));

  DeletionVectors info;
  info.dv_entries[0][0] = {iceberg::ice_tea::DeletionVectorInfo{dv_file.path, dv_file.offset, dv_file.length, data_path}};

  std::vector<SelectionVector<int32_t>> expected_selection_vectors = {
      SelectionVector<int32_t>(std::vector<int32_t>{0, 2}),
      SelectionVector<int32_t>(std::vector<int32_t>{0, 2}), // 0 and 2 are relative to row_position 3 (rows 3 and 5)
  };

  std::vector<std::shared_ptr<IcebergBatch>> result_batches = GetResult(std::move(input_stream), std::move(info));

  CompareResultWithExpected(arrow_input_batches, result_batches, expected_selection_vectors);
  EXPECT_EQ(counting_logger_->DeletedRowsCount(), 2);
}

}  // namespace
}  // namespace iceberg
