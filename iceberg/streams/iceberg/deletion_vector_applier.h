#pragma once
#include <iostream>

#include <iceberg/common/logger.h>
#include <iceberg/deletion_vector.h>
#include <iceberg/puffin.h>
#include <iceberg/tea_scan.h>

#include <map>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/common/defer.h"
#include "iceberg/common/error.h"
#include "iceberg/common/fs/file_reader_provider.h"
#include "iceberg/result.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"
#include "iceberg/streams/iceberg/plan.h"

namespace iceberg {

struct DeletionVectors {
  // partition_id -> layer_id -> list of DVs
  std::map<PartitionId, std::map<LayerId, std::vector<iceberg::ice_tea::DeletionVectorInfo>>> dv_entries;

  const std::map<LayerId, std::vector<iceberg::ice_tea::DeletionVectorInfo>>* GetDeletionVectors(
      PartitionId partition) const {
    auto it = dv_entries.find(partition);
    if (it == dv_entries.end()) {
      return nullptr;
    }
    return &it->second;
  }
};

class DeletionVectorApplier : public IcebergStream {
 public:
  explicit DeletionVectorApplier(IcebergStreamPtr input, DeletionVectors dv_infos,
                                 std::shared_ptr<const IFileReaderProvider> file_reader_provider,
                                 std::shared_ptr<ILogger> logger = nullptr)
      : input_(input), dv_infos_(std::move(dv_infos)), file_reader_provider_(file_reader_provider), logger_(logger) {
    Ensure(input_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": input is nullptr");
    Ensure(file_reader_provider_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": file_reader_provider is nullptr");
  }

  std::shared_ptr<IcebergBatch> ReadNext() override {
    auto batch = input_->ReadNext();
    if (!batch) {
      return nullptr;
    }

    if (logger_) {
      logger_->Log("", "events:deletion_vector:start_batch");
    }
    Defer defer([&]() {
      if (logger_) {
        logger_->Log("", "events:deletion_vector:end_batch");
      }
    });

    if (!CanReuseState(*batch)) {
      UpdateState(*batch);
    }

    if (current_state_->dv) {
      ApplyDeletionVector(*batch, *current_state_->dv);
    }

    return batch;
  }

 private:
  bool CanReuseState(const IcebergBatch& batch) const {
    return current_state_.has_value() && current_state_->partition == batch.GetPartition() &&
           current_state_->layer == batch.GetLayer() && current_state_->path == batch.GetPath();
  }

  void UpdateState(const IcebergBatch& batch) {
    current_state_.emplace(CurrentFileState{batch.GetPartition(), batch.GetLayer(), batch.GetPath(), nullptr});

    const auto* dvs_for_partition = dv_infos_.GetDeletionVectors(batch.GetPartition());
    if (!dvs_for_partition) {
      return;
    }

    auto it = dvs_for_partition->find(batch.GetLayer());
    if (it == dvs_for_partition->end()) {
      return;
    }

    for (const auto& dv_info : it->second) {
      if (dv_info.referenced_data_file == batch.GetPath()) {
        if (current_state_->dv) {
          logger_->Log("More than one DV found for file: " + dv_info.referenced_data_file,
                       "warning:deletion_vector:duplicate");
        }
        current_state_->dv = LoadDeletionVector(dv_info);
      }
    }
  }

  std::shared_ptr<DeletionVector> LoadDeletionVector(const iceberg::ice_tea::DeletionVectorInfo& dv_info) const {
    try {
      auto input_file = ValueSafe(file_reader_provider_->OpenRaw(dv_info.path));
      auto footer = ValueSafe(PuffinFile::ReadFooter(input_file));

      auto deserialized_footer = footer.GetDeserializedFooter();
      auto it = std::ranges::lower_bound(deserialized_footer.blobs, dv_info.offset, {},
                                         &PuffinFile::Footer::BlobMetadata::offset);
      if (it == deserialized_footer.blobs.end() || it->offset != dv_info.offset || it->length != dv_info.length) {
        if (logger_) {
          logger_->Log("Blob not found at offset " + std::to_string(dv_info.offset),
                       "warning:deletion_vector:load_failed");
        }
        return nullptr;
      }
      auto buffer = ValueSafe(input_file->ReadAt(dv_info.offset, dv_info.length));
      std::string blob_data(reinterpret_cast<const char*>(buffer->data()), buffer->size());
      return std::make_shared<DeletionVector>(*it, std::move(blob_data));
    } catch (const std::exception& e) {
      if (logger_) {
        logger_->Log(e.what(), "error:deletion_vector:load_failed");
      }
      return nullptr;
    }
  }

  void ApplyDeletionVector(IcebergBatch& batch, const DeletionVector& dv) {
    auto& selection_vector = batch.GetSelectionVector();
    uint64_t start_pos = batch.GetRowPosition();
    uint64_t num_rows = batch.GetRecordBatch()->num_rows();
    auto relative_rows = dv.GetRelativeElems(start_pos, start_pos + num_rows - 1);

    if (relative_rows.empty()) {
      return;
    }

    int32_t deleted_count = selection_vector.DeleteIfEqual(relative_rows.begin(), relative_rows.end());
    if (logger_) {
      logger_->Log(std::to_string(deleted_count), "metrics:deletion_vector:deleted_rows");
    }
  }

  struct CurrentFileState {
    PartitionId partition;
    LayerId layer;
    std::string path;
    std::shared_ptr<DeletionVector> dv;
  };

  IcebergStreamPtr input_;
  DeletionVectors dv_infos_;
  std::shared_ptr<const IFileReaderProvider> file_reader_provider_;
  std::optional<CurrentFileState> current_state_;
  std::shared_ptr<ILogger> logger_;
};

}  // namespace iceberg
