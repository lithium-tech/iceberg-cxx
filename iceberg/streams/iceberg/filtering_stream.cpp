#include "iceberg/streams/iceberg/filtering_stream.h"

namespace iceberg {

FilteringStream::FilteringStream(StreamPtr<ArrowBatchWithRowPosition> filter_stream,
                                 StreamPtr<ArrowBatchWithRowPosition> data_stream,
                                 std::shared_ptr<const ice_filter::IRowFilter> row_filter,
                                 const PartitionLayerFile& partition_layer_file, std::shared_ptr<ILogger> logger)
    : filter_stream_(filter_stream),
      data_stream_(data_stream),
      row_filter_(row_filter),
      partition_layer_file_(partition_layer_file),
      logger_(logger) {
  Ensure(data_stream != nullptr, std::string(__PRETTY_FUNCTION__) + ": data_stream is nullptr");
  Ensure(filter_stream != nullptr, std::string(__PRETTY_FUNCTION__) + ": filter_stream is nullptr");
  Ensure(row_filter != nullptr, std::string(__PRETTY_FUNCTION__) + ": row_filter is nullptr");
}

std::shared_ptr<IcebergBatch> FilteringStream::ReadNext() {
  while (true) {
    auto batch_filter = filter_stream_->ReadNext();
    if (batch_filter == nullptr) {
      if (logger_) {
        logger_->Log(std::to_string(rows_skipped_), "metrics:rows:filtered_out");
      }
      return nullptr;
    }
    auto selection_vector = row_filter_->ApplyFilter(batch_filter);
    if (selection_vector.Size() == 0) {
      rows_skipped_ += batch_filter->GetRecordBatch()->num_rows();
      continue;
    }
    while (rows_skipped_ > 0) {
      auto batch_data = data_stream_->ReadNext();
      Ensure(batch_data != nullptr,
             std::string(__PRETTY_FUNCTION__) + ": internal error. data_stream has ended before filter_stream");
      rows_skipped_ -= batch_data->GetRecordBatch()->num_rows();
    }
    Ensure(rows_skipped_ == 0, std::string(__PRETTY_FUNCTION__) +
                                   ": internal error. data_stream batch sizes don't match filter_stream batch sizes.");

    auto batch_data = data_stream_->ReadNext();
    auto result_batch = Concatenate(batch_data, batch_filter);

    BatchWithSelectionVector batch_with_vec(result_batch->GetRecordBatch(), std::move(selection_vector));
    PartitionLayerFilePosition result_state(partition_layer_file_, result_batch->row_position);
    return std::make_shared<IcebergBatch>(std::move(batch_with_vec), std::move(result_state));
  }
}

std::shared_ptr<ArrowBatchWithRowPosition> Concatenate(std::shared_ptr<ArrowBatchWithRowPosition> a,
                                                       std::shared_ptr<ArrowBatchWithRowPosition> b) {
  Ensure(a->row_position == b->row_position,
         std::string(__PRETTY_FUNCTION__) + ": batches' row positions are different");
  auto a_record = a->GetRecordBatch();
  auto b_record = b->GetRecordBatch();
  Ensure(a_record != nullptr, std::string(__PRETTY_FUNCTION__) + ": record batch is nullptr");
  Ensure(b_record != nullptr, std::string(__PRETTY_FUNCTION__) + ": record batch is nullptr");

  Ensure(a_record->num_rows() == b_record->num_rows(),
         std::string(__PRETTY_FUNCTION__) + ": batches have different sizes");

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(a_record->num_columns() + b_record->num_columns());

  for (int i = 0; i < a_record->num_columns(); ++i) {
    fields.push_back(a_record->schema()->field(i));
  }
  for (int i = 0; i < b_record->num_columns(); ++i) {
    fields.push_back(b_record->schema()->field(i));
  }

  auto schema = arrow::schema(fields);

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(a_record->num_columns() + b_record->num_columns());

  for (int i = 0; i < a_record->num_columns(); ++i) {
    arrays.push_back(a_record->column(i));
  }

  for (int i = 0; i < b_record->num_columns(); ++i) {
    arrays.push_back(b_record->column(i));
  }

  auto record_batch = arrow::RecordBatch::Make(schema, a_record->num_rows(), arrays);

  return std::make_shared<ArrowBatchWithRowPosition>(record_batch, a->row_position);
}

}  // namespace iceberg
