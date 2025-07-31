#pragma once

#include "arrow/array/util.h"
#include "iceberg/streams/arrow/batch_with_row_number.h"
#include "iceberg/streams/arrow/stream.h"
#include "iceberg/streams/iceberg/mapper.h"

namespace iceberg {

class MaterializeNulls : public IStream<ArrowBatchWithRowPosition> {
 public:
  MaterializeNulls(StreamPtr<ArrowBatchWithRowPosition> stream, std::vector<int>&& remaining_field_ids,
                   std::shared_ptr<const FieldIdMapper> mapper,
                   std::shared_ptr<const std::map<int, std::shared_ptr<arrow::DataType>>> arrow_types_map)
      : stream_(stream),
        remaining_field_ids_(std::move(remaining_field_ids)),
        mapper_(mapper),
        arrow_types_map_(arrow_types_map) {}

  std::shared_ptr<ArrowBatchWithRowPosition> ReadNext() override {
    auto batch = stream_->ReadNext();
    if (!batch) {
      return nullptr;
    }

    auto record_batch = batch->GetRecordBatch();
    for (int field_id : remaining_field_ids_) {
      auto name = mapper_->FieldIdToColumnName(field_id);
      auto arrow_type = arrow_types_map_->at(field_id);
      auto array_of_nulls = arrow::MakeArrayOfNull(arrow_type, record_batch->num_rows()).ValueOrDie();
      record_batch = record_batch->AddColumn(record_batch->num_columns(), std::move(name), array_of_nulls).ValueOrDie();
    }
    return std::make_shared<ArrowBatchWithRowPosition>(record_batch, batch->row_position);
  }

 private:
  StreamPtr<ArrowBatchWithRowPosition> stream_;
  const std::vector<int> remaining_field_ids_;
  std::shared_ptr<const FieldIdMapper> mapper_;
  std::shared_ptr<const std::map<int, std::shared_ptr<arrow::DataType>>> arrow_types_map_;
};

}  // namespace iceberg
