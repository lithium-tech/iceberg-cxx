#include "iceberg/common/rg_metadata.h"

#include <stdexcept>

namespace iceberg {

int64_t RowGroupMetaToFileOffset(const parquet::RowGroupMetaData& meta) {
  // RowGroup.file_offset is deprecated (see https://issues.apache.org/jira/browse/PARQUET-2078 and
  // https://github.com/apache/parquet-format/issues/394).
  // We use it for backward compatibility. In case if it is not set, we fallback to data_page_offset
  auto row_group_offset = meta.file_offset();

  // row_group_offset is not set
  if (row_group_offset == 0) {
    // offset is used only for balancing, so we do not care if data_page_offset matches file_offset
    row_group_offset = meta.ColumnChunk(0)->data_page_offset();

    if (row_group_offset == 0) {
      throw std::runtime_error("File offset is not set in file");
    }
  }

  return row_group_offset;
}

}  // namespace iceberg
