#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "arrow/array/array_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "gen/src/batch.h"
#include "gen/src/generators.h"
#include "gen/src/processor.h"

namespace gen {

namespace {

std::shared_ptr<arrow::BooleanArray> GetInversedMask(const std::shared_ptr<arrow::BooleanArray>& mask) {
  arrow::BooleanBuilder builder;
  std::shared_ptr<arrow::BooleanArray> array;
  for (int64_t i = 0; i < mask->length(); ++i) {
    if (!mask->IsNull(i)) {
      if (auto status = builder.Append(!mask->Value(i)); !status.ok()) {
        throw status;
      }
    }
  }

  if (auto status = builder.Finish(&array); !status.ok()) {
    throw status;
  }

  return array;
}

}  // namespace

class EqualityDeleteProcessor : public IProcessor {
 public:
  explicit EqualityDeleteProcessor(const std::vector<std::string>& selected_columns, std::shared_ptr<Generator> gen)
      : selected_columns_(selected_columns), generator_(gen) {}

  arrow::Status Process(BatchPtr batch) override {
    auto projection = batch->GetProjection(selected_columns_);
    auto mask = generator_->Generate(batch).ValueOrDie();
    auto bool_mask = std::static_pointer_cast<arrow::BooleanArray>(mask);

    if (!bool_mask || !bool_mask->type()->Equals(arrow::boolean())) {
      return arrow::Status::ExecutionError("Mask is not a BooleanArray");
    }

    auto processed_batch = BuildBatchWithMask(batch, bool_mask);

    auto inversed_mask = GetInversedMask(bool_mask);
    auto inverted_processed_batch = BuildBatchWithMask(batch, inversed_mask);

    ARROW_RETURN_NOT_OK(delete_processor->Process(processed_batch));
    ARROW_RETURN_NOT_OK(result_data_processor->Process(inverted_processed_batch));

    return arrow::Status::OK();
  }

  void SetDeleteProcessor(std::shared_ptr<IProcessor> child) { delete_processor = child; }

  void SetResultDataProcessor(std::shared_ptr<IProcessor> child) { result_data_processor = child; }

 private:
  BatchPtr BuildBatchWithMask(BatchPtr batch, std::shared_ptr<arrow::BooleanArray> mask) {
    std::vector<std::shared_ptr<arrow::Array>> filtered_arrays;
    filtered_arrays.reserve(selected_columns_.size());

    for (size_t i = 0; i < selected_columns_.size(); ++i) {
      filtered_arrays.push_back(arrow::compute::Filter(batch->ColumnByName(selected_columns_[i]), mask)->make_array());
    }

    int64_t num_rows = 0;
    for (int64_t i = 0; i < mask->length(); ++i) {
      if (!mask->IsNull(i)) {
        num_rows += mask->Value(i);
      }
    }

    return std::make_shared<Batch>(num_rows, selected_columns_, filtered_arrays);
  }

  std::vector<std::string> selected_columns_;
  std::shared_ptr<Generator> generator_;
  std::shared_ptr<IProcessor> delete_processor;
  std::shared_ptr<IProcessor> result_data_processor;
};

}  // namespace gen
