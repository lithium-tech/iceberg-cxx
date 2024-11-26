#include "iceberg/parquet/io.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <arrow/util/iterator.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/schema.h>

#include <optional>
#include <stdexcept>
#include <string>

#include "arrow/io/file.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/transforms.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_writer.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace iceberg::parquet_io {

namespace {

std::vector<std::shared_ptr<arrow::RecordBatch>> FilterTable(const std::shared_ptr<arrow::Table>& table,
                                                             const std::vector<int>& row_indices) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  arrow::RecordBatchVector selected_batches;

  for (int index : row_indices) {
    if (index < 0 || index >= table->num_rows()) {
      throw std::runtime_error("out of range " + std::to_string(index) + " " + std::to_string(table->num_rows()));
    }

    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::TableBatchReader reader(*table);

    if (reader.ReadNext(&batch).ok() && batch != nullptr) {
      selected_batches.push_back(batch->Slice(index, 1));
    }
  }
  return batches;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> GetRecordBatchesFromTable(const std::shared_ptr<arrow::Table>& table) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

  arrow::TableBatchReader reader(*table);

  std::shared_ptr<arrow::RecordBatch> batch;
  while (reader.ReadNext(&batch).ok() && batch != nullptr) {
    batches.push_back(batch);
  }

  return batches;
}

class ParquetWriter {
 public:
  ParquetWriter(const std::shared_ptr<arrow::fs::FileSystem>& fs, const std::string& filename,
                const std::shared_ptr<arrow::Schema>& arrow_schema)
      : fs_(fs) {
    arrow::Result<std::shared_ptr<arrow::io::OutputStream>> maybe_outfile = fs_->OpenOutputStream(filename);
    if (!maybe_outfile.ok()) {
      throw maybe_outfile.status();
    }
    outfile_ = maybe_outfile.ValueOrDie();
    parquet::WriterProperties::Builder builder;
    auto properties = builder.build();

    if (auto status = parquet::arrow::ToParquetSchema(arrow_schema.get(), *properties, &parquet_schema_);
        !status.ok()) {
      throw std::runtime_error("can not open file");
    }
    std::shared_ptr<parquet::schema::GroupNode> group_node_ptr = std::shared_ptr<parquet::schema::GroupNode>(
        const_cast<parquet::schema::GroupNode*>(parquet_schema_->group_node()), [](parquet::schema::GroupNode*) {});

    parquet_writer_ = parquet::ParquetFileWriter::Open(outfile_, group_node_ptr);
  }

  arrow::Status WriteRecordBatch(std::shared_ptr<arrow::RecordBatch> record_batch) {
    if (arrow_writer_ == nullptr) {
      ARROW_RETURN_NOT_OK(parquet::arrow::FileWriter::Make(arrow::default_memory_pool(), std::move(parquet_writer_),
                                                           record_batch->schema(),
                                                           parquet::default_arrow_writer_properties(), &arrow_writer_));
    }

    ARROW_RETURN_NOT_OK(record_batch->Validate());

    return arrow_writer_->WriteRecordBatch(*record_batch);
  }

  ~ParquetWriter() {
    if (auto status = Close(); !status.ok()) {
    }
  }

 private:
  arrow::Status Close() {
    if (parquet_writer_) {
      parquet_writer_->Close();
      parquet_writer_.reset();
    }

    if (arrow_writer_) {
      ARROW_RETURN_NOT_OK(arrow_writer_->Close());
      arrow_writer_ = nullptr;
    }
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::fs::FileSystem> fs_;
  std::shared_ptr<arrow::io::OutputStream> outfile_;
  std::unique_ptr<parquet::arrow::FileWriter> arrow_writer_;
  std::unique_ptr<parquet::ParquetFileWriter> parquet_writer_;
  std::shared_ptr<parquet::SchemaDescriptor> parquet_schema_;
};

}  // namespace

std::string WriteTask::GenerateDataFileName(const std::string& extension) const {
  return "00000-" + std::to_string(task_id) + "-" + write_uuid.ToString() + "." + extension;
}

std::string WriteTask::GenerateDataFilePath(const std::string& prefix, const std::string& extension) const {
  if (partition_key) {
    return prefix + "/" + partition_key->ToPath() + "-" + GenerateDataFileName(extension);
  } else {
    return prefix + "/" + GenerateDataFileName(extension);
  }
}

std::vector<PartitionKey> DeterminePartitions(const PartitionSpec& partition_spec,
                                              const std::shared_ptr<arrow::Table>& table) {
  std::vector<PartitionKey> result;
  for (size_t i = 0; i < table->num_rows(); ++i) {
    PartitionKey identifier;
    identifier.partition_values_.resize(partition_spec.fields.size());

    for (size_t j = 0; j < partition_spec.fields.size(); ++j) {
      if (auto maybe_scalar_value = table->column(partition_spec.fields[j].source_id)->GetScalar(i);
          maybe_scalar_value.ok()) {
        auto result_value = partition_spec.fields[j].transform->Transform(maybe_scalar_value.ValueUnsafe());
        identifier.partition_values_[partition_spec.fields[j].field_id] = std::move(result_value);
      } else {
        throw std::runtime_error("Incorrect value after transform");
      }
    }
    result.push_back(std::move(identifier));
  }
  return result;
}
std::vector<DataFile> WriteToFiles(std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& file_prefix,
                                   const std::vector<WriteTask>& tasks) {
  std::vector<DataFile> result;
  for (auto& task : tasks) {
    auto output_filename = task.GenerateDataFilePath(file_prefix, "parquet");
    ParquetWriter writer(fs, output_filename, task.schema);
    for (const auto& batch : task.record_batches) {
      if (auto status = writer.WriteRecordBatch(batch); !status.ok()) {
        throw std::runtime_error("can not write batch");
      }
    }

    DataFile data_file;
    data_file.file_format = "parquet";
    data_file.file_path = output_filename;
    data_file.content = DataFile::FileContent::kData;

    result.push_back(std::move(data_file));
  }
  return result;
}

std::vector<DataFile> ArrowTableToDataFiles(const std::shared_ptr<arrow::Table>& table, const std::string& file_prefix,
                                            std::shared_ptr<arrow::fs::FileSystem> fs, Uuid write_uuid, int counter,
                                            const std::optional<PartitionSpec>& partition_spec) {
  std::vector<WriteTask> write_tasks;
  if (partition_spec.has_value()) {
    auto partitions = DeterminePartitions(partition_spec.value(), table);

    // TODO(k.i.vedernikov): speedup this
    std::unordered_map<PartitionKey, std::vector<int>, PartitionKeyHasher> partition_to_row_indices;
    for (size_t i = 0; i < partitions.size(); ++i) {
      partition_to_row_indices[partitions[i]].push_back(i);
    }

    for (const auto& partition_info : partition_to_row_indices) {
      write_tasks.push_back(WriteTask{
          .write_uuid = write_uuid,
          .task_id = counter++,
          .schema = table->schema(),
          .record_batches = FilterTable(table, partition_info.second),
          .sort_order_id = std::nullopt,
          .partition_key = partition_info.first,
      });
    }
  } else {
    write_tasks.push_back(WriteTask{
        .write_uuid = write_uuid,
        .task_id = counter++,
        .schema = table->schema(),
        .record_batches = GetRecordBatchesFromTable(table),
        .sort_order_id = std::nullopt,
        .partition_key = std::nullopt,
    });
  }
  return WriteToFiles(fs, file_prefix, write_tasks);
}

}  // namespace iceberg::parquet_io
