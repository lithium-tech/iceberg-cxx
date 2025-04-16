#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "iceberg/common/batch.h"
#include "iceberg/result.h"
#include "iceberg/streams/arrow/batch_with_row_number.h"
#include "iceberg/streams/arrow/stream.h"

namespace iceberg {

class ProjectionStream : public IStream<ArrowBatchWithRowPosition> {
 public:
  ProjectionStream(std::map<std::string, std::string> old_name_to_new_name, StreamPtr<ArrowBatchWithRowPosition> input)
      : old_name_to_new_name_(std::move(old_name_to_new_name)), input_(input) {
    Ensure(input != nullptr, std::string(__PRETTY_FUNCTION__) + ": input is nullptr");
  }

  std::shared_ptr<ArrowBatchWithRowPosition> ReadNext() override {
    auto batch = input_->ReadNext();
    return MakeProjection(old_name_to_new_name_, batch);
  }

  static std::shared_ptr<ArrowBatchWithRowPosition> MakeProjection(
      const std::map<std::string, std::string>& old_name_to_new_name,
      std::shared_ptr<ArrowBatchWithRowPosition> batch) {
    if (!batch) {
      return nullptr;
    }

    auto get_projection_wo_renaming = [&]() {
      std::vector<int> indices_result;
      const auto& input_schema = batch->GetRecordBatch()->schema();
      for (const auto& [old_name, new_name] : old_name_to_new_name) {
        auto index = input_schema->GetFieldIndex(old_name);
        if (index != -1) {
          indices_result.emplace_back(index);
        }
      }
      return batch->GetRecordBatch()->SelectColumns(indices_result);
    };

    auto projection_wo_renaming = iceberg::ValueSafe(get_projection_wo_renaming());
    auto result_after_renaming = [&]() {
      const auto& old_schema = projection_wo_renaming->schema();
      auto names = old_schema->field_names();
      for (auto& name : names) {
        name = old_name_to_new_name.at(name);
      }
      auto new_schema = iceberg::ValueSafe(old_schema->WithNames(names));
      auto result = iceberg::ValueSafe(projection_wo_renaming->ReplaceSchema(new_schema));
      return result;
    };

    auto batch_result = result_after_renaming();
    return std::make_shared<ArrowBatchWithRowPosition>(std::move(batch_result), batch->row_position);
  }

 private:
  const std::map<std::string, std::string> old_name_to_new_name_;
  StreamPtr<ArrowBatchWithRowPosition> input_;
};

}  // namespace iceberg
