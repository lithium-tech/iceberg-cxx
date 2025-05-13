#pragma once

#include <iceberg/common/logger.h>

#include <memory>
#include <string>
#include <vector>

#include "iceberg/streams/arrow/row_group_reader.h"
#include "iceberg/streams/arrow/stream.h"
#include "parquet/arrow/reader.h"

namespace iceberg {

class ParquetFileReader : public IStream<ArrowBatchWithRowPosition> {
 public:
  ParquetFileReader(std::shared_ptr<parquet::arrow::FileReader> input_file, const std::vector<int>& row_groups,
                    const std::vector<int>& columns, std::shared_ptr<ILogger> logger = nullptr)
      : input_file_(input_file), row_groups_(row_groups), columns_(columns), logger_(logger) {
    Ensure(input_file != nullptr, std::string(__PRETTY_FUNCTION__) + ": batch is nullptr");
  }

  std::shared_ptr<ArrowBatchWithRowPosition> ReadNext() override {
    while (true) {
      if (!current_stream_) {
        if (next_position_in_row_group_array_ == row_groups_.size()) {
          return nullptr;
        }
        current_stream_ = std::make_shared<RowGroupReaderWithRowNumber>(
            input_file_, row_groups_.at(next_position_in_row_group_array_), columns_);
        ++next_position_in_row_group_array_;
        if (logger_) {
          logger_->Log(std::to_string(1), "metrics:row_groups:read");
        }
      }
      auto batch = current_stream_->ReadNext();
      if (!batch) {
        current_stream_.reset();
        continue;
      }
      return batch;
    }
  }

 private:
  std::shared_ptr<parquet::arrow::FileReader> input_file_;
  std::vector<int> row_groups_;
  std::vector<int> columns_;

  size_t next_position_in_row_group_array_ = 0;

  StreamPtr<ArrowBatchWithRowPosition> current_stream_;
  std::shared_ptr<ILogger> logger_;
};

}  // namespace iceberg
