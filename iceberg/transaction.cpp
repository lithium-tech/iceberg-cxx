#include "iceberg/transaction.h"

#include <arrow/filesystem/filesystem.h>

#include <memory>

#include "iceberg/common/error.h"
#include "iceberg/manifest_entry.h"
// TODO(k.i.vedernikov): make iceberg-parquet as independent library
#include "iceberg/parquet/io.h"

namespace iceberg {

Transaction::Transaction(Table* table, std::shared_ptr<TableMetadataV2> table_metadata,
                         std::shared_ptr<arrow::fs::FileSystem> fs, bool auto_commit)
    : table_(table), table_metadata_(table_metadata), fs_(fs), auto_commit_(auto_commit) {}

void Transaction::AppendTable(const std::shared_ptr<arrow::Table>& table) {
  std::vector<DataFile> data_files;
  if (table_metadata_ && !table_metadata_->partition_specs.empty()) {
    data_files = parquet_io::ArrowTableToDataFiles(table, table_->Location(), fs_, uuid_generator_.CreateRandom(),
                                                   cnt_files_, *table_metadata_->partition_specs[0]);
  } else {
    data_files = parquet_io::ArrowTableToDataFiles(table, table_->Location(), fs_, uuid_generator_.CreateRandom(),
                                                   cnt_files_, std::nullopt);
  }
  cnt_files_ += data_files.size();
  data_files_.insert(data_files_.end(), data_files.begin(), data_files.end());

  if (auto_commit_) {
    Commit();
  }
}

void Transaction::Commit() {
  Ensure(!commited_, "Transaction can not be commited twice.");

  commited_ = true;
  table_->DoCommit(data_files_);
}

}  // namespace iceberg
