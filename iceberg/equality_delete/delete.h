#pragma once

#include <memory>
#include <set>
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "iceberg/equality_delete/common.h"
#include "iceberg/equality_delete/utils.h"

namespace iceberg {

class EqualityDelete {
 public:
  using Layer = int;

  virtual ~EqualityDelete() = default;

  virtual arrow::Status Add(const std::vector<std::shared_ptr<arrow::Array>>& arrays, uint64_t rows_count,
                            Layer delete_layer) = 0;

  virtual size_t Size() const = 0;

  virtual bool IsDeleted(const std::vector<std::shared_ptr<arrow::Array>>& arrays, uint64_t row,
                         Layer data_layer) const = 0;
};

using EqualityDeletePtr = std::unique_ptr<EqualityDelete>;

inline size_t GetSize(const EqualityDeletePtr& ptr) {
  if (!ptr) {
    return 0;
  } else {
    return ptr->Size();
  }
}

EqualityDeletePtr MakeEqualityDelete(std::shared_ptr<arrow::Schema> schema, bool use_specialized_deletes,
                                     uint64_t num_rows, const std::shared_ptr<MemoryState>& shared_state);

}  // namespace iceberg
