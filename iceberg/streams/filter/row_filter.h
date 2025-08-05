#pragma once

#include "iceberg/common/selection_vector.h"
#include "iceberg/streams/arrow/batch_with_row_number.h"

namespace iceberg::ice_filter {

class IRowFilter {
 public:
  explicit IRowFilter(std::vector<int32_t> involved_field_ids) : involved_field_ids_(std::move(involved_field_ids)) {}

  virtual SelectionVector<int32_t> ApplyFilter(std::shared_ptr<ArrowBatchWithRowPosition> batch) const = 0;

  virtual ~IRowFilter() = default;

  const std::vector<int32_t>& GetInvolvedFieldIds() const { return involved_field_ids_; }

 private:
  std::vector<int32_t> involved_field_ids_;
};

}  // namespace iceberg::ice_filter
