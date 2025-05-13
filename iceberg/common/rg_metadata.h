#pragma once

#include "parquet/metadata.h"

namespace iceberg {

int64_t RowGroupMetaToFileOffset(const parquet::RowGroupMetaData& meta);

}  // namespace iceberg
