#pragma once

#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/compute/api.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "gen/src/generators.h"
#include "gen/src/processor.h"

namespace gen {

template <typename Generator>
class PositionalDeleteProcessor : public IProcessor {
 public:
  explicit PositionalDeleteProcessor(const std::string& file_name, Generator&& gen, RandomDevice& random_device)
      : random_device_(random_device), file_name_(file_name), gen_(gen) {}

  arrow::Status Process(BatchPtr batch) override {
    int num_rows = batch->NumRows();

    arrow::BooleanBuilder mask_builder;
    std::shared_ptr<arrow::BooleanArray> mask;

    arrow::StringBuilder file_names_builder;
    std::shared_ptr<arrow::StringArray> file_names;

    arrow::Int32Builder indices_builder;
    std::shared_ptr<arrow::Int32Array> indices;

    for (int i = 0; i < num_rows; ++i) {
      if (gen_(random_device_) > 0) {
        ARROW_RETURN_NOT_OK(indices_builder.Append(shift_ + i));
        ARROW_RETURN_NOT_OK(file_names_builder.Append(file_name_));
        ARROW_RETURN_NOT_OK(mask_builder.Append(false));
      } else {
        ARROW_RETURN_NOT_OK(mask_builder.Append(true));
      }
    }

    ARROW_RETURN_NOT_OK(mask_builder.Finish(&mask));
    ARROW_RETURN_NOT_OK(file_names_builder.Finish(&file_names));
    ARROW_RETURN_NOT_OK(indices_builder.Finish(&indices));

    auto positional_delete_batch = std::make_shared<Batch>(
        num_rows, std::vector<std::string>{"1", "2"}, std::vector<std::shared_ptr<arrow::Array>>{file_names, indices});

    ARROW_RETURN_NOT_OK(delete_processor_->Process(positional_delete_batch));

    if (result_data_processor_) {
      std::vector<std::shared_ptr<arrow::Array>> filtered_batch;
      for (int i = 0; i < batch->NumColumns(); ++i) {
        filtered_batch.push_back(arrow::compute::Filter(batch->Column(i), mask)->make_array());
      }

      auto result_positional_delete = std::make_shared<Batch>(num_rows, batch->Schema()->field_names(), filtered_batch);

      ARROW_RETURN_NOT_OK(result_data_processor_->Process(result_positional_delete));
    }

    shift_ += num_rows;
    return arrow::Status::OK();
  }

  void SetDeleteProcessor(std::shared_ptr<IProcessor> child) { delete_processor_ = child; }

  void SetResultDataProcessor(std::shared_ptr<IProcessor> child) { result_data_processor_ = child; }

 private:
  std::shared_ptr<IProcessor> delete_processor_;
  std::shared_ptr<IProcessor> result_data_processor_;

  RandomDevice& random_device_;

  std::string file_name_;
  Generator gen_;
  int shift_ = 0;
};

}  // namespace gen
