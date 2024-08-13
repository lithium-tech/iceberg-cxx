#pragma once

#include <memory>
#include <string>

#include "iceberg/src/schema.h"

namespace iceberg {

class Table {
 public:
  virtual ~Table() = default;

  // virtual const Snapshot& CurrentSnapshot() const = 0;
  // virtual const Snapshot& GetSnapshot​(int64_t snapshotId);
  virtual const std::string& Location() const = 0;
  virtual const std::string& Name() const = 0;
  virtual std::shared_ptr<Schema> GetSchema() const = 0;
  // AppendFiles NewAppend();
  // TableScan NewScan();
  // UpdateSchema updateSchema();
};

}  // namespace iceberg
