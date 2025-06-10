#include "iceberg/streams/compute/projection.h"

namespace iceberg::compute {

static int ColumnIndexByName(const std::shared_ptr<arrow::Schema>& schema, const std::string& name) {
  return schema->GetFieldIndex(name);
}

static int ColumnIndexByName(const std::shared_ptr<arrow::Schema>& schema, std::shared_ptr<arrow::Field> field) {
  return schema->GetFieldIndex(field->name());
}

template <typename T>
std::shared_ptr<arrow::Schema> ProjectionImpl(const std::shared_ptr<arrow::Schema>& src_schema,
                                              const std::vector<T>& column_names, bool throw_if_column_not_found) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(column_names.size());
  for (auto& name : column_names) {
    int pos = ColumnIndexByName(src_schema, name);
    if (pos < 0) {
      if (throw_if_column_not_found) {
        throw std::runtime_error("No column in block " + src_schema->ToString());
      }
      continue;
    }
    fields.push_back(src_schema->field(pos));
  }

  return std::make_shared<arrow::Schema>(std::move(fields));
}

std::shared_ptr<arrow::Schema> Projection(const std::shared_ptr<arrow::Schema>& src_schema,
                                          const std::vector<std::string>& column_names,
                                          bool throw_if_column_not_found) {
  return ProjectionImpl(src_schema, column_names, throw_if_column_not_found);
}

template <typename T>
static std::shared_ptr<arrow::RecordBatch> ProjectionImpl(const std::shared_ptr<arrow::RecordBatch>& src_batch,
                                                          const std::vector<T>& column_names,
                                                          bool throw_if_column_not_found) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(column_names.size());
  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(column_names.size());

  auto src_schema = src_batch->schema();
  for (auto& name : column_names) {
    int pos = ColumnIndexByName(src_schema, name);
    if (pos < 0) {
      if (throw_if_column_not_found) {
        throw std::runtime_error("No column in block " + src_schema->ToString());
      }
      continue;
    }
    fields.push_back(src_schema->field(pos));
    columns.push_back(src_batch->column(pos));
  }

  return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), src_batch->num_rows(),
                                  std::move(columns));
}

std::shared_ptr<arrow::RecordBatch> Projection(const std::shared_ptr<arrow::RecordBatch>& src_batch,
                                               const std::vector<std::string>& dst_schema,
                                               bool throw_if_column_not_found) {
  return ProjectionImpl(src_batch, dst_schema, throw_if_column_not_found);
}

std::shared_ptr<arrow::RecordBatch> Projection(const std::shared_ptr<arrow::RecordBatch>& src_batch,
                                               const std::shared_ptr<arrow::Schema>& dst_schema,
                                               bool throw_if_column_not_found) {
  return ProjectionImpl(src_batch, dst_schema->fields(), throw_if_column_not_found);
}

}  // namespace iceberg::compute
