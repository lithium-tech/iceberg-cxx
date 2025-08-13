#pragma once

#include <memory>
#include <string>

#include "arrow/record_batch.h"
#include "iceberg/common/error.h"

namespace iceberg {

struct ArrowBatchWithRowPosition {
  std::shared_ptr<arrow::RecordBatch> batch;
  int64_t row_position;  // position of first batch row in file (numbering from 0)

  std::shared_ptr<arrow::RecordBatch> GetRecordBatch() const { return batch; }

  ArrowBatchWithRowPosition() = delete;
  ArrowBatchWithRowPosition(std::shared_ptr<arrow::RecordBatch> b, int64_t r) : batch(b), row_position(r) {
    Ensure(batch != nullptr, std::string(__PRETTY_FUNCTION__) + ": batch is nullptr");
  }
};

}  // namespace iceberg
