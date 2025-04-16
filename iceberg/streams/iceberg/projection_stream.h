#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "iceberg/common/batch.h"
#include "iceberg/streams/arrow/batch_with_row_number.h"
#include "iceberg/streams/arrow/projection_stream.h"
#include "iceberg/streams/arrow/stream.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"

namespace iceberg {

class IcebergProjectionStream : public IStream<IcebergBatch> {
 public:
  IcebergProjectionStream(std::map<std::string, std::string> old_name_to_new_name, StreamPtr<IcebergBatch> input)
      : old_name_to_new_name_(std::move(old_name_to_new_name)), input_(input) {
    Ensure(input_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": input is nullptr");
  }

  std::shared_ptr<IcebergBatch> ReadNext() override {
    auto batch = input_->ReadNext();
    if (!batch) {
      return nullptr;
    }
    auto batch_with_row_number =
        std::make_shared<ArrowBatchWithRowPosition>(batch->GetRecordBatch(), batch->GetRowPosition());

    auto batch_after_projection = ProjectionStream::MakeProjection(old_name_to_new_name_, batch_with_row_number);
    BatchWithSelectionVector batch_with_selection_vector(batch_with_row_number->batch,
                                                         std::move(batch->GetSelectionVector()));

    return std::make_shared<IcebergBatch>(std::move(batch_with_selection_vector),
                                          batch->GetPartitionLayerFilePosition());
  }

 private:
  const std::map<std::string, std::string> old_name_to_new_name_;
  StreamPtr<IcebergBatch> input_;
};

}  // namespace iceberg
