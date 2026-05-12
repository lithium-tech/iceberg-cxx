#pragma once

#include <iceberg/common/logger.h>
#include <iceberg/deletion_vector.h>

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/common/defer.h"
#include "iceberg/common/error.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"

namespace iceberg {

class DeletionVectorApplier : public IcebergStream {
 public:
  explicit DeletionVectorApplier(IcebergStreamPtr input, std::shared_ptr<DeletionVector> dv,
                                 std::shared_ptr<ILogger> logger = nullptr)
      : input_(input), dv_(std::move(dv)), logger_(logger) {
    Ensure(input_ != nullptr, "input is nullptr");
    Ensure(dv_ != nullptr, "dv is nullptr");
  }

  std::shared_ptr<IcebergBatch> ReadNext() override {
    auto batch = input_->ReadNext();
    if (!batch) {
      return nullptr;
    }

    if (logger_) logger_->Log("", "events:deletion_vector:start_batch");
    Defer defer([&]() {
      if (logger_) logger_->Log("", "events:deletion_vector:end_batch");
    });

    ApplyDeletionVector(*batch, *dv_);

    return batch;
  }

 private:
  static std::vector<int32_t> GetRelativeElems(const DeletionVector& dv, uint64_t start, uint64_t end) {
    Ensure(end - start <= static_cast<uint64_t>(std::numeric_limits<int32_t>::max()), "batch is too large");

    auto abs_elems = dv.GetElems(start, end);
    std::vector<int32_t> relative;
    relative.reserve(abs_elems.size());
    for (uint64_t pos : abs_elems) {
      relative.push_back(static_cast<int32_t>(pos - start));
    }
    return relative;
  }

  void ApplyDeletionVector(IcebergBatch& batch, const DeletionVector& dv) {
    auto& sv = batch.GetSelectionVector();
    uint64_t start = batch.GetRowPosition();
    uint64_t end = start + static_cast<uint64_t>(batch.GetRecordBatch()->num_rows()) - 1;
    auto relative = GetRelativeElems(dv, start, end);

    if (!relative.empty()) {
      int32_t count = sv.DeleteIfEqual(relative.begin(), relative.end());
      if (logger_) {
        logger_->Log(std::to_string(count), "metrics:deletion_vector:deleted_rows");
      }
    }
  }

  IcebergStreamPtr input_;
  std::shared_ptr<DeletionVector> dv_;
  std::shared_ptr<ILogger> logger_;
};

}  // namespace iceberg
