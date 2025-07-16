#pragma once

#include <map>

#include "iceberg/literals.h"
#include "iceberg/streams/arrow/batch_with_row_number.h"
#include "iceberg/streams/arrow/stream.h"

namespace iceberg {
class DefaultValueApplier : public IStream<ArrowBatchWithRowPosition> {
 public:
  DefaultValueApplier(StreamPtr<ArrowBatchWithRowPosition> input,
                      std::shared_ptr<const std::map<int, Literal>> default_value_map,
                      std::vector<std::pair<int, std::string>>&& remaining_field_ids_with_names)
      : input_(input),
        default_value_map_(default_value_map),
        remaining_field_ids_with_names_(std::move(remaining_field_ids_with_names)) {
    Ensure(input != nullptr, std::string(__PRETTY_FUNCTION__) + ": input is nullptr");
    Ensure(default_value_map != nullptr, std::string(__PRETTY_FUNCTION__) + ": default_value_map is nullptr");
  }

  std::shared_ptr<ArrowBatchWithRowPosition> ReadNext() override {
    auto batch = input_->ReadNext();
    if (!batch) {
      return nullptr;
    }

    auto arrow_batch = batch->GetRecordBatch();

    for (const auto &[field_id, name] : remaining_field_ids_with_names_) {
        arrow_batch = arrow_batch->AddColumn(arrow_batch->num_columns(), name, default_value_map_->at(field_id).MakeColumn(arrow_batch->num_rows())).ValueOrDie();
    }
    return std::make_shared<ArrowBatchWithRowPosition>(arrow_batch, batch->row_position);
  }

 private:
  StreamPtr<ArrowBatchWithRowPosition> input_;
  std::shared_ptr<const std::map<int, Literal>> default_value_map_;
  std::vector<std::pair<int, std::string>> remaining_field_ids_with_names_;
};
}  // namespace iceberg
