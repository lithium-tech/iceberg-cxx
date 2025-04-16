#pragma once

#include <memory>
#include <utility>

#include "arrow/record_batch.h"
#include "iceberg/common/batch.h"
#include "iceberg/common/selection_vector.h"
#include "iceberg/streams/arrow/stream.h"
#include "iceberg/streams/iceberg/plan.h"

namespace iceberg {

class IcebergBatch : public PartitionLayerFilePosition {
 public:
  std::shared_ptr<arrow::RecordBatch> GetRecordBatch() const { return batch_; }

  SelectionVector<int32_t>& GetSelectionVector() { return selection_vector_; }
  const SelectionVector<int32_t>& GetSelectionVector() const { return selection_vector_; }

  IcebergBatch() = delete;

  IcebergBatch(BatchWithSelectionVector b, PartitionLayerFilePosition state)
      : PartitionLayerFilePosition(std::move(state)),
        batch_(b.GetBatch()),
        selection_vector_(std::move(b.GetSelectionVector())) {}

 private:
  std::shared_ptr<arrow::RecordBatch> batch_;
  SelectionVector<int32_t> selection_vector_;
};

using IcebergStream = IStream<IcebergBatch>;
using IcebergStreamPtr = StreamPtr<IcebergBatch>;

}  // namespace iceberg
