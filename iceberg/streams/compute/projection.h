#pragma once

#include <memory>
#include <string>

#include "arrow/api.h"

namespace iceberg::compute {

std::shared_ptr<arrow::Schema> Projection(const std::shared_ptr<arrow::Schema>& src_schema,
                                          const std::vector<std::string>& column_names, bool throw_if_column_not_found);
std::shared_ptr<arrow::Schema> Projection(const std::shared_ptr<arrow::Schema>& src_schema,
                                          const std::vector<std::string_view>& column_names,
                                          bool throw_if_column_not_found = true);

std::shared_ptr<arrow::RecordBatch> Projection(const std::shared_ptr<arrow::RecordBatch>& src_batch,
                                               const std::vector<std::string>& column_names,
                                               bool throw_if_column_not_found = true);
std::shared_ptr<arrow::RecordBatch> Projection(const std::shared_ptr<arrow::RecordBatch>& src_batch,
                                               const std::vector<std::string_view>& column_names,
                                               bool throw_if_column_not_found = true);

std::shared_ptr<arrow::RecordBatch> Projection(const std::shared_ptr<arrow::RecordBatch>& src_batch,
                                               const std::shared_ptr<arrow::Schema>& dst_schema,
                                               bool throw_if_column_not_found = true);

}  // namespace iceberg::compute
