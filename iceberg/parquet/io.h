#pragma once

#include <arrow/filesystem/filesystem.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <memory>
#include <vector>

#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transforms.h"
#include "iceberg/uuid.h"

namespace iceberg::parquet_io {

struct WriteTask {
  Uuid write_uuid;
  int task_id;
  std::shared_ptr<arrow::Schema> schema;
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  std::optional<int> sort_order_id;
  std::optional<PartitionKey> partition_key;

  std::string GenerateDataFileName(const std::string& extension) const;

  std::string GenerateDataFilePath(const std::string& prefix, const std::string& extension) const;
};

std::vector<PartitionKey> DeterminePartitions(const PartitionSpec& partition_spec,
                                              const std::shared_ptr<arrow::Table>& table);

std::vector<DataFile> ArrowTableToDataFiles(const std::shared_ptr<arrow::Table>& table, const std::string& file_prefix,
                                            std::shared_ptr<arrow::fs::FileSystem> fs, Uuid write_uuid, int counter,
                                            const std::optional<PartitionSpec>& partition_spec);

}  // namespace iceberg::parquet_io
