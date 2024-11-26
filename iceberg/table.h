#pragma once

#include <arrow/table.h>

#include <memory>
#include <string>

#include "iceberg/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

class Table {
 public:
  virtual ~Table() = default;

  // virtual const Snapshot& CurrentSnapshot() const = 0;
  // virtual const Snapshot& GetSnapshot​(int64_t snapshotId);
  virtual const std::string& Location() const = 0;
  virtual const std::string& Name() const = 0;
  virtual std::shared_ptr<Schema> GetSchema() const = 0;
  virtual std::vector<std::string> GetFilePathes() const = 0;
  // AppendFiles NewAppend();
  // TableScan NewScan();
  // UpdateSchema updateSchema();

  virtual void AppendTable(std::shared_ptr<arrow::Table> table) = 0;

 protected:
  friend class Transaction;

  virtual void DoCommit(const std::vector<DataFile>& file_updates) = 0;
};

}  // namespace iceberg
