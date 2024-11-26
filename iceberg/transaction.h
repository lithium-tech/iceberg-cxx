#pragma once

#include <arrow/filesystem/filesystem.h>
#include <arrow/table.h>

#include <memory>

#include "iceberg/manifest_entry.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/uuid.h"

namespace iceberg {

class Transaction {
 public:
  explicit Transaction(Table* table, std::shared_ptr<TableMetadataV2> table_metadata,
                       std::shared_ptr<arrow::fs::FileSystem> fs, bool auto_commit = false);

  // TODO(k.i.vedernikov): add other methods to transaction

  void AppendTable(const std::shared_ptr<arrow::Table>& table);

  void Commit();

 private:
  bool commited_ = false;
  Table* table_;
  std::shared_ptr<TableMetadataV2> table_metadata_;

  std::vector<DataFile> data_files_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;

  bool auto_commit_ = false;
  int cnt_files_ = 0;
  UuidGenerator uuid_generator_;
};

}  // namespace iceberg
