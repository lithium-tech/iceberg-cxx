#pragma once

#include <memory>

#include "arrow/record_batch.h"

namespace iceberg {

template <typename T>
class IStream {
 public:
  virtual std::shared_ptr<T> ReadNext() = 0;

  virtual ~IStream() = default;
};

template <typename T>
using StreamPtr = std::shared_ptr<IStream<T>>;

using IBatchStream = IStream<arrow::RecordBatch>;
using BatchStreamPtr = StreamPtr<arrow::RecordBatch>;

}  // namespace iceberg
