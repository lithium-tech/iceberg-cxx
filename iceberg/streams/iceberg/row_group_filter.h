#pragma once

#include "parquet/metadata.h"

namespace iceberg {

class IRowGroupFilter {
 public:
  virtual bool CanSkipRowGroup(const parquet::RowGroupMetaData& meta) const = 0;

  virtual ~IRowGroupFilter() = default;
};

}  // namespace iceberg
