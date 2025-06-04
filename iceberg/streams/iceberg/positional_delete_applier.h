#pragma once

#include <iceberg/common/logger.h>
#include <iceberg/tea_scan.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/common/defer.h"
#include "iceberg/common/fs/file_reader_provider.h"
#include "iceberg/positional_delete/positional_delete.h"
#include "iceberg/result.h"
#include "iceberg/streams/arrow/error.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"
#include "iceberg/streams/iceberg/plan.h"

namespace iceberg {

// TODO(gmusya): use interface for getting positional deletes
struct PositionalDeletes {
  std::map<PartitionId, std::map<LayerId, std::vector<iceberg::ice_tea::PositionalDeleteInfo>>> delete_entries;

  std::map<LayerId, std::vector<std::string>> GetDeleteUrls(PartitionId partition) const {
    std::map<LayerId, std::vector<std::string>> urls;
    for (const auto& [l, partition_delete_entries] : delete_entries.at(partition)) {
      for (const auto& del_entry : partition_delete_entries) {
        urls[l].push_back(del_entry.path);
      }
    }

    return urls;
  }
};

class PositionalDeleteApplier : public IcebergStream {
 public:
  explicit PositionalDeleteApplier(IcebergStreamPtr input, PositionalDeletes pos_del_infos,
                                   std::shared_ptr<const IFileReaderProvider> file_reader_provider,
                                   std::shared_ptr<PositionalDeleteStream::BasicRowGroupFilter> filter =
                                       std::make_shared<PositionalDeleteStream::BasicRowGroupFilter>(),
                                   std::shared_ptr<ILogger> logger = nullptr)
      : input_(input),
        pos_del_infos_(std::move(pos_del_infos)),
        file_reader_provider_(file_reader_provider),
        filter_(filter),
        logger_(logger) {
    Ensure(input_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": input is nullptr");
    Ensure(file_reader_provider_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": file_reader_provider is nullptr");
  }

  std::shared_ptr<IcebergBatch> ReadNext() override {
    auto batch = input_->ReadNext();
    if (!batch) {
      return nullptr;
    }

    if (logger_) {
      logger_->Log("", "events:positional:start_batch");
    }
    Defer defer([&]() {
      if (logger_) {
        logger_->Log("", "events:positional:end_batch");
      }
    });

    bool paths_are_increasing =
        current_state_.has_value() && ((batch->GetPath() > current_state_->GetPath()) ||
                                       (batch->GetPath() == current_state_->GetPath() &&
                                        batch->GetRowPosition() >= current_state_->GetRowPosition()));

    bool can_reuse_state =
        current_state_.has_value() && (current_state_->GetPartition() == batch->GetPartition()) && paths_are_increasing;

    if (!can_reuse_state) {
      current_state_.reset();
      positional_delete_.reset();

      if (!pos_del_infos_.delete_entries.contains(batch->GetPartition())) {
        return batch;
      }

      auto open_file_lambda = [file_reader_provider = this->file_reader_provider_](const std::string& path) {
        auto result = iceberg::ValueSafe(file_reader_provider->Open(path));

        return result;
      };

      positional_delete_ = std::make_shared<PositionalDeleteStream>(pos_del_infos_.GetDeleteUrls(batch->GetPartition()),
                                                                    open_file_lambda, filter_, logger_);
    }

    if (positional_delete_) {
      DeleteRows delete_rows = positional_delete_->GetDeleted(
          batch->GetPath(), batch->GetRowPosition(), batch->GetRowPosition() + batch->GetRecordBatch()->num_rows(),
          batch->GetLayer());
      for (auto& row : delete_rows) {
        row -= batch->GetRowPosition();
      }

      int32_t deleted_rows = batch->GetSelectionVector().DeleteIfEqual(delete_rows.begin(), delete_rows.end());
      if (logger_) {
        logger_->Log(std::to_string(deleted_rows), "metrics:positional:deleted_rows");
      }

      {
        // note that this differs from batch->GetPartitionLayerFilePosition()
        current_state_ = PartitionLayerFilePosition(batch->GetPartitionLayerFile(),
                                                    batch->GetRowPosition() + batch->GetRecordBatch()->num_rows());
      }
    }

    return batch;
  }

 private:
  IcebergStreamPtr input_;
  const PositionalDeletes pos_del_infos_;

  std::shared_ptr<PositionalDeleteStream> positional_delete_;

  std::shared_ptr<const IFileReaderProvider> file_reader_provider_;
  std::optional<PartitionLayerFilePosition> current_state_;
  std::shared_ptr<PositionalDeleteStream::BasicRowGroupFilter> filter_;
  std::shared_ptr<ILogger> logger_;
};

}  // namespace iceberg
