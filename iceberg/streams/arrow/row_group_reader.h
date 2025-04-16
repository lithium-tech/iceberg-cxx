#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/record_batch.h"
#include "iceberg/result.h"
#include "iceberg/streams/arrow/batch_with_row_number.h"
#include "iceberg/streams/arrow/stream.h"
#include "parquet/arrow/reader.h"

namespace iceberg {

class RowGroupReader : public IBatchStream {
 public:
  RowGroupReader(std::shared_ptr<parquet::arrow::FileReader> input_file, int row_group_to_read,
                 const std::vector<int>& columns) {
    Ensure(input_file != nullptr, std::string(__PRETTY_FUNCTION__) + ": input_file is nullptr");
    iceberg::Ensure(input_file->GetRecordBatchReader({row_group_to_read}, columns, &record_batch_reader_));
  }

  std::shared_ptr<arrow::RecordBatch> ReadNext() override {
    std::shared_ptr<arrow::RecordBatch> batch;
    iceberg::Ensure(record_batch_reader_->ReadNext(&batch));
    return batch;
  }

 private:
  std::shared_ptr<arrow::RecordBatchReader> record_batch_reader_;
};

class RowGroupReaderWithRowNumber : public IStream<ArrowBatchWithRowPosition> {
 public:
  RowGroupReaderWithRowNumber(std::shared_ptr<parquet::arrow::FileReader> input_file, int row_group_to_read,
                              const std::vector<int>& columns)
      : rg_reader_(std::make_shared<RowGroupReader>(input_file, row_group_to_read, columns)) {
    Ensure(input_file != nullptr, std::string(__PRETTY_FUNCTION__) + ": input_file is nullptr");
    auto parquet_reader = input_file->parquet_reader();
    Ensure(parquet_reader != nullptr, std::string(__PRETTY_FUNCTION__) + ": parquet_reader is nullptr");
    // it is slow if there are thousand of row groups in one file
    const auto& meta = input_file->parquet_reader()->metadata();
    Ensure(meta != nullptr, std::string(__PRETTY_FUNCTION__) + ": meta is nullptr");
    for (int i = 0; i < row_group_to_read; ++i) {
      auto rg_meta = meta->RowGroup(i);
      Ensure(rg_meta != nullptr, std::string(__PRETTY_FUNCTION__) + ": rg_meta is nullptr");
      current_row_ += rg_meta->num_rows();
    }
  }

  std::shared_ptr<ArrowBatchWithRowPosition> ReadNext() override {
    auto batch = rg_reader_->ReadNext();
    if (!batch) {
      return nullptr;
    }
    auto res = std::make_shared<ArrowBatchWithRowPosition>(batch, current_row_);
    current_row_ += batch->num_rows();
    return res;
  }

 private:
  int64_t current_row_ = 0;
  BatchStreamPtr rg_reader_;
};

}  // namespace iceberg
