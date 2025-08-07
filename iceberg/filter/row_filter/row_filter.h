#pragma once

#include <algorithm>
#include <memory>
#include <string>

#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "gandiva/filter.h"
#include "iceberg/common/logger.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/row_filter/registry.h"

namespace iceberg::filter {

class IArrowFieldResolver {
 public:
  virtual arrow::Result<std::shared_ptr<arrow::Field>> CreateField(const std::string& column_name) const = 0;

  virtual ~IArrowFieldResolver() = default;
};

class TrivialArrowFieldResolver : public filter::IArrowFieldResolver {
 public:
  explicit TrivialArrowFieldResolver(std::shared_ptr<arrow::Schema> schema) : schema_(schema) {}

  arrow::Result<std::shared_ptr<arrow::Field>> CreateField(const std::string& column_name) const override {
    std::string column_name_lower = ToLower(column_name);
    const auto& fields = schema_->fields();
    for (const auto& field : fields) {
      if (ToLower(field->name()) == column_name_lower) {
        return field;
      }
    }
    return arrow::Status::ExecutionError("Column ", column_name, " not found");
  }

 private:
  static std::string ToLower(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](char ch) { return std::tolower(ch); });
    return str;
  }

  std::shared_ptr<arrow::Schema> schema_;
};

class RowFilter {
 public:
  explicit RowFilter(iceberg::filter::NodePtr root, std::shared_ptr<iceberg::ILogger> logger = nullptr);

  arrow::Status BuildFilter(std::shared_ptr<const IArrowFieldResolver> arrow_field_resolver,
                            std::shared_ptr<const IGandivaFunctionRegistry> function_registry,
                            std::shared_ptr<arrow::Schema> batch_schema);

  arrow::Result<std::shared_ptr<gandiva::SelectionVector>> ApplyFilter(
      std::shared_ptr<arrow::RecordBatch> record_batch) const;

  // Used only for testing/debugging purposes
  std::string GetFilterString() const { return last_condition_as_str_; }

 private:
  std::string last_schema_as_str_;
  std::string last_condition_as_str_;
  std::shared_ptr<gandiva::Filter> filter_;
  iceberg::filter::NodePtr root_;
  std::shared_ptr<iceberg::ILogger> logger_;
};

}  // namespace iceberg::filter
