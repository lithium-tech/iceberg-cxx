#pragma once

#include "arrow/api.h"
#include "iceberg/common/logger.h"
#include "iceberg/streams/filter/row_filter.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"

namespace iceberg {
class FilteringStream : public IcebergStream {
 public:
  FilteringStream(StreamPtr<ArrowBatchWithRowPosition> filter_stream, StreamPtr<ArrowBatchWithRowPosition> data_stream,
                  std::shared_ptr<const ice_filter::IRowFilter> row_filter,
                  const PartitionLayerFile& partition_layer_file, std::shared_ptr<ILogger> logger = nullptr);

  std::shared_ptr<IcebergBatch> ReadNext() override;

 private:
  StreamPtr<ArrowBatchWithRowPosition> filter_stream_;
  StreamPtr<ArrowBatchWithRowPosition> data_stream_;
  std::shared_ptr<const ice_filter::IRowFilter> row_filter_;
  const PartitionLayerFile partition_layer_file_;
  std::shared_ptr<ILogger> logger_;

  int64_t rows_skipped_ = 0;
};

std::shared_ptr<ArrowBatchWithRowPosition> Concatenate(std::shared_ptr<ArrowBatchWithRowPosition> a,
                                                       std::shared_ptr<ArrowBatchWithRowPosition> b);

}  // namespace iceberg
