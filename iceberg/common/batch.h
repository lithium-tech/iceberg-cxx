#pragma once

#include <memory>
#include <utility>

#include "arrow/record_batch.h"
#include "iceberg/common/selection_vector.h"

namespace iceberg {

class BatchWithSelectionVector {
 public:
  BatchWithSelectionVector(std::shared_ptr<arrow::RecordBatch> batch, SelectionVector<int32_t> selection_vector)
      : batch_(batch), selection_vector_(std::move(selection_vector)) {
    if (!batch_) {
      throw std::runtime_error("BatchWithSelectionVector: batch is null");
    }
  }

  BatchWithSelectionVector(const BatchWithSelectionVector&) = default;
  BatchWithSelectionVector& operator=(const BatchWithSelectionVector&) = default;

  BatchWithSelectionVector(BatchWithSelectionVector&&) = default;
  BatchWithSelectionVector& operator=(BatchWithSelectionVector&&) = default;

  std::shared_ptr<arrow::RecordBatch> GetBatch() const { return batch_; }
  const SelectionVector<int32_t>& GetSelectionVector() const { return selection_vector_; }

  SelectionVector<int32_t>& GetSelectionVector() { return selection_vector_; }

 private:
  std::shared_ptr<arrow::RecordBatch> batch_;
  SelectionVector<int32_t> selection_vector_;
};

}  // namespace iceberg
