#include "iceberg/equality_delete/handler.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "iceberg/common/logger.h"
#include "iceberg/equality_delete/common.h"
#include "iceberg/equality_delete/delete.h"
#include "iceberg/streams/arrow/error.h"
#include "parquet/arrow/reader.h"
#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"

namespace iceberg {

namespace {

std::vector<int> GetParquetFieldIds(const std::shared_ptr<parquet::FileMetaData>& file_metadata) {
  auto parquet_schema = file_metadata->schema();
  auto group_node = parquet_schema->group_node();
  auto field_count = group_node->field_count();
  std::vector<int> fields;
  fields.reserve(field_count);
  for (int i = 0; i < field_count; ++i) {
    auto field = group_node->field(i);
    fields.emplace_back(field->field_id());
  }
  return fields;
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> PrepareRecordBatchReader(
    const std::shared_ptr<parquet::arrow::FileReader>& reader, const std::set<FieldId>& field_ids) {
  auto file_metadata = reader->parquet_reader()->metadata();
  auto parquet_ids = GetParquetFieldIds(file_metadata);

  std::map<FieldId, int> field_id_to_position;
  for (size_t i = 0; i < parquet_ids.size(); ++i) {
    FieldId id = parquet_ids[i];
    if (field_ids.contains(id)) {
      field_id_to_position.emplace(id, i);
    }
  }
  // https://iceberg.apache.org/spec/#equality-delete-files
  // If a column was added to a table and later used as a delete column in an equality delete file, the column value is
  // read for older data files using normal projection rules (defaults to null)
  // TODO(gmusya): support missing column
  for (const auto& id : field_ids) {
    if (!field_id_to_position.contains(id)) {
      return arrow::Status::ExecutionError("Column with field id ", id, " is not found in equality delete file");
    }
  }

  std::vector<int> columns;
  columns.reserve(field_id_to_position.size());
  for (auto& [field_id, pos] : field_id_to_position) {
    columns.emplace_back(pos);
  }

  auto num_row_groups = file_metadata->num_row_groups();
  std::vector<int> row_groups(num_row_groups);
  std::iota(row_groups.begin(), row_groups.end(), 0);

  std::shared_ptr<arrow::RecordBatchReader> record_batch_reader;
  ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(row_groups, columns, &record_batch_reader));
  return record_batch_reader;
}

arrow::Status ParseEqualityDeleteFile(std::shared_ptr<arrow::RecordBatchReader> record_batch_reader,
                                      std::unique_ptr<EqualityDelete>& equality_delete,
                                      EqualityDeleteHandler::Layer delete_layer) {
  while (true) {
    std::shared_ptr<arrow::RecordBatch> record_batch;
    ARROW_RETURN_NOT_OK(record_batch_reader->ReadNext(&record_batch));
    if (!record_batch) {
      break;
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < record_batch->num_columns(); ++i) {
      arrays.push_back(record_batch->column(i));
    }
    ARROW_RETURN_NOT_OK(equality_delete->Add(arrays, record_batch->num_rows(), delete_layer));
  }
  return arrow::Status::OK();
}
}  // namespace

bool EqualityDeleteHandler::PrepareDeletesForFile(Layer data_layer) {
  current_data_layer_ = data_layer;

  deletes_for_current_row_group_.clear();
  deletes_for_current_row_group_.reserve(materialized_deletes_.size());

  for (const auto& [field_ids, delete_ptr] : materialized_deletes_) {
    if (delete_ptr->Size() > 0) {
      deletes_for_current_row_group_.emplace_back(delete_ptr.get(), field_ids);
    }
  }

  return deletes_for_current_row_group_.size() > 0;
}

void EqualityDeleteHandler::PrepareDeletesForBatch(
    const std::map<FieldId, std::shared_ptr<arrow::Array>>& field_id_to_array) {
  prepared_batches_.clear();
  for (const auto& [delete_ptr, field_ids] : deletes_for_current_row_group_) {
    PreparedBatch batch;
    batch.reserve(field_ids.size());
    for (const auto& id : field_ids) {
      batch.push_back(field_id_to_array.at(id));
    }
    prepared_batches_.emplace_back(delete_ptr, std::move(batch));
    // TODO(gmusya): validate schema/types
  }
}

arrow::Status EqualityDeleteHandler::AppendDelete(const std::string& url, const std::vector<FieldId>& field_ids_vec,
                                                  Layer delete_layer) {
  ARROW_ASSIGN_OR_RAISE(auto reader, open_url_method_(url));
  if (logger_) {
    logger_->Log(std::to_string(1), "metrics:equality:files_read");
  }

  int64_t rows_in_file = reader->parquet_reader()->metadata()->num_rows();
  if (current_rows_ + rows_in_file > max_rows_) {
    return arrow::Status::ExecutionError("Equality delete rows limit exceeded (", current_rows_ + rows_in_file, "/",
                                         max_rows_, ")");
  }
  if (logger_) {
    logger_->Log(std::to_string(rows_in_file), "metrics:equality:rows_read");
  }

  std::set<FieldId> field_ids(field_ids_vec.begin(), field_ids_vec.end());
  ARROW_ASSIGN_OR_RAISE(auto record_batch_reader, PrepareRecordBatchReader(reader, field_ids));

  if (!materialized_deletes_.contains(field_ids)) {
    materialized_deletes_.emplace(field_ids, MakeEqualityDelete(record_batch_reader->schema(), use_specialized_deletes_,
                                                                rows_in_file, shared_state_));
  }
  auto& eq_del_ptr = materialized_deletes_.at(field_ids);

  ARROW_RETURN_NOT_OK(ParseEqualityDeleteFile(record_batch_reader, eq_del_ptr, delete_layer));

  current_rows_ = 0;
  for (auto& [_, deletes] : materialized_deletes_) {
    if (deletes) {
      current_rows_ += deletes->Size();
    }
  }

  if (logger_) {
    logger_->Log(std::to_string(current_rows_), "metrics:equality:current_materialized_rows");
  }
  if (logger_) {
    logger_->Log(std::to_string(static_cast<double>(shared_state_->Allocated()) / (1024.0 * 1024.0)),
                 "metrics:equality:current_mb_materialized");
  }

  return arrow::Status::OK();
}

absl::flat_hash_set<int> EqualityDeleteHandler::GetEqualityDeleteFieldIds() const {
  absl::flat_hash_set<int> result;

  for (const auto& [field_ids, _] : materialized_deletes_) {
    for (const auto field_id : field_ids) {
      result.insert(field_id);
    }
  }

  return result;
}

EqualityDeleteHandler::EqualityDeleteHandler(ReaderMethodType get_reader_method, const Config& config,
                                             std::shared_ptr<iceberg::ILogger> logger)
    : max_rows_(config.max_rows),
      use_specialized_deletes_(config.use_specialized_deletes),
      open_url_method_(get_reader_method),
      shared_state_(std::make_shared<MemoryState>(1024 * 1024 * config.equality_delete_max_mb_size,
                                                  config.throw_if_memory_limit_exceeded)),
      logger_(logger) {}

bool EqualityDeleteHandler::IsDeleted(uint64_t row) const {
  iceberg::Ensure(current_data_layer_.has_value(),
                  std::string(__PRETTY_FUNCTION__) + ": current_data_layer is not set");

  for (const auto& [delete_ptr, arrow_arrays] : prepared_batches_) {
    if (delete_ptr->IsDeleted(arrow_arrays, row, *current_data_layer_)) {
      return true;
    }
  }
  return false;
}

}  // namespace iceberg
