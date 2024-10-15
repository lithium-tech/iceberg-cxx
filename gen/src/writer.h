#pragma once

#include <iostream>
#include <memory>
#include <string>

#include "arrow/csv/api.h"
#include "arrow/csv/options.h"
#include "arrow/csv/writer.h"
#include "arrow/io/file.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "gen/src/log.h"
#include "gen/src/processor.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_writer.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace gen {

class Writer {
 public:
  virtual arrow::Status WriteRecordBatch(std::shared_ptr<arrow::RecordBatch> record_batch) = 0;

  virtual ~Writer() = default;
};

class ParquetWriter : public Writer {
 public:
  ParquetWriter(const std::string& filename, const std::shared_ptr<parquet::schema::GroupNode>& schema)
      : outfile_([&filename]() {
          auto maybe_outfile = arrow::io::FileOutputStream::Open(filename);
          if (!maybe_outfile.ok()) {
            throw maybe_outfile.status();
          }
          return maybe_outfile.ValueUnsafe();
        }()),
        parquet_writer_(parquet::ParquetFileWriter::Open(outfile_, schema)) {}

  arrow::Status WriteRecordBatch(std::shared_ptr<arrow::RecordBatch> record_batch) override {
    if (arrow_writer_ == nullptr) {
      ARROW_RETURN_NOT_OK(parquet::arrow::FileWriter::Make(arrow::default_memory_pool(), std::move(parquet_writer_),
                                                           record_batch->schema(),
                                                           parquet::default_arrow_writer_properties(), &arrow_writer_));
    }
    ARROW_RETURN_NOT_OK(record_batch->Validate());

    return arrow_writer_->WriteRecordBatch(*record_batch);
  }

  ~ParquetWriter() {
    auto status = Close();
    LOG_NOT_OK(status);
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

  std::shared_ptr<arrow::io::FileOutputStream> outfile_;
  std::unique_ptr<parquet::ParquetFileWriter> parquet_writer_;
  std::unique_ptr<parquet::arrow::FileWriter> arrow_writer_;
};

class CSVWriter : public Writer {
 public:
  CSVWriter(const std::string& filename, const std::shared_ptr<arrow::Schema> schema,
            const arrow::csv::WriteOptions& options = arrow::csv::WriteOptions::Defaults())
      : schema_(schema), outfile_([&filename]() {
          auto maybe_outfile = arrow::io::FileOutputStream::Open(filename);
          if (!maybe_outfile.ok()) {
            throw maybe_outfile.status();
          }
          return maybe_outfile.ValueUnsafe();
        }()) {
    auto maybe_writer = arrow::csv::MakeCSVWriter(outfile_, schema, options);
    if (!maybe_writer.ok()) {
      throw maybe_writer;
    }
    arrow_writer_ = *maybe_writer;
  }

  arrow::Status WriteRecordBatch(std::shared_ptr<arrow::RecordBatch> record_batch) override {
    ARROW_RETURN_NOT_OK(record_batch->Validate());
    if (!record_batch->schema()->Equals(schema_)) {
      return arrow::Status::ExecutionError("Record batch schema does not match CSVWriter schema:\n",
                                           record_batch->schema()->ToString(), "\n!=\n", schema_->ToString());
    }
    return arrow_writer_->WriteRecordBatch(*record_batch);
  }

  ~CSVWriter() {
    auto status = Close();
    LOG_NOT_OK(status);
  }

 private:
  arrow::Status Close() {
    if (arrow_writer_) {
      ARROW_RETURN_NOT_OK(arrow_writer_->Close());
      arrow_writer_ = nullptr;
    }
    return arrow::Status::OK();
  }

  const std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::io::FileOutputStream> outfile_;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> arrow_writer_;
};

class BatchSizeMaker {
 public:
  BatchSizeMaker(int64_t batch_size, int64_t total_rows) : batch_size_(batch_size), total_rows_(total_rows) {}

  int64_t NextBatchSize() {
    int64_t batch_size = std::min(batch_size_, total_rows_ - rows_done_);
    rows_done_ += batch_size;
    return batch_size;
  }

 private:
  int64_t rows_done_ = 0;
  const int64_t batch_size_;
  const int64_t total_rows_;
};

class WriterProcessor : public IProcessor {
 public:
  WriterProcessor(std::shared_ptr<Writer> writer)
      : writer_(writer) {}

  arrow::Status Process(BatchPtr batch) override {
    ARROW_ASSIGN_OR_RAISE(auto arrow_batch, batch->GetArrowBatch(batch->Schema()->field_names()));
    return writer_->WriteRecordBatch(arrow_batch);
  }


 private:
  std::shared_ptr<Writer> writer_;
};

}  // namespace gen
