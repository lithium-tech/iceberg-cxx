#pragma once

#include <memory>
#include <sstream>
#include <vector>

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

namespace gen {

class Batch;

using BatchPtr = std::shared_ptr<Batch>;

class Batch {
 public:
  explicit Batch(int32_t num_rows) : num_rows_(num_rows) {}

  Batch(int32_t num_rows, const std::vector<std::string>& column_names,
        const std::vector<std::shared_ptr<arrow::Array>>& arrays)
      : num_rows_(num_rows), column_names_(column_names), arrays_(arrays) {}

  int32_t NumRows() const { return num_rows_; }

  int32_t NumColumns() const { return arrays_.size(); }

  std::shared_ptr<arrow::Array> Column(int32_t i) const { return arrays_[i]; }

  std::shared_ptr<arrow::Schema> Schema() const {
    arrow::FieldVector fields;
    for (size_t i = 0; i < column_names_.size(); ++i) {
      auto new_field = std::make_shared<arrow::Field>(column_names_[i], arrays_[i]->type());
      fields.emplace_back(new_field);
    }
    return std::make_shared<arrow::Schema>(fields);
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetArrowBatch(const std::vector<std::string>& columns) const {
    ARROW_ASSIGN_OR_RAISE(auto proj, GetProjection(columns));

    return arrow::RecordBatch::Make(proj->Schema(), proj->num_rows_, proj->arrays_);
  }

  arrow::Result<BatchPtr> GetProjection(const std::vector<std::string>& column_names) const {
    std::vector<std::string> result_column_name;
    std::vector<std::shared_ptr<arrow::Array>> result_arrays;
    int32_t result_rows = num_rows_;

    for (const auto& column_name : column_names) {
      auto it = std::find(column_names_.begin(), column_names_.end(), column_name);
      if (it == column_names_.end()) {
        return arrow::Status::ExecutionError("Column not found: ", column_name);
      }

      const auto index = it - column_names_.begin();

      result_column_name.emplace_back(column_names_[index]);
      result_arrays.emplace_back(arrays_[index]);
      result_rows = arrays_[index]->length();
    }

    return std::make_shared<Batch>(result_rows, result_column_name, result_arrays);
  }

  std::vector<std::shared_ptr<arrow::DataType>> ColumnTypes() const {
    std::vector<std::shared_ptr<arrow::DataType>> result;
    for (const auto& array : arrays_) {
      result.emplace_back(array->type());
    }
    return result;
  }

  arrow::Result<BatchPtr> DropColumnByIndex(uint64_t index) const {
    if (index >= column_names_.size()) {
      return arrow::Status::ExecutionError("Column index out of range: ", index);
    }

    std::vector<std::string> columns = column_names_;
    columns.erase(columns.begin() + index);

    return GetProjection(columns);
  }

  arrow::Result<BatchPtr> AddColumn(const std::string& column_name, const std::shared_ptr<arrow::Array>& array) const {
    if (std::find(column_names_.begin(), column_names_.end(), column_name) != column_names_.end()) {
      return arrow::Status::ExecutionError("Column already exists: ", column_name);
    }

    auto result = std::make_shared<Batch>(num_rows_, column_names_, arrays_);
    result->column_names_.emplace_back(column_name);
    result->arrays_.emplace_back(array);
    result->num_rows_ = array->length();
    return result;
  }

  std::string ToString() const {
    std::stringstream result;
    for (size_t i = 0; i < column_names_.size(); ++i) {
      result << column_names_[i] << ": " << arrays_[i]->ToString() << std::endl;
    }
    return result.str();
  }

 private:
  int32_t num_rows_;
  std::vector<std::string> column_names_;
  std::vector<std::shared_ptr<arrow::Array>> arrays_;
};

}  // namespace gen
