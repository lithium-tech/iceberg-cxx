#pragma once

#include <arrow/array/array_base.h>
#include <arrow/status.h>
#include <iceberg/common/logger.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/common/defer.h"
#include "iceberg/common/fs/file_reader_provider.h"
#include "iceberg/equality_delete/handler.h"
#include "iceberg/result.h"
#include "iceberg/streams/arrow/error.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"
#include "iceberg/streams/iceberg/mapper.h"
#include "iceberg/streams/iceberg/plan.h"
#include "iceberg/tea_scan.h"

namespace iceberg {

// TODO(gmusya): use interface for getting equality deletes
struct EqualityDeletes {
  std::map<PartitionId, std::map<LayerId, std::vector<iceberg::ice_tea::EqualityDeleteInfo>>> partlayer_to_deletes;

  std::vector<int> GetFieldIds(PartitionLayer state) const {
    if (!partlayer_to_deletes.contains(state.GetPartition())) {
      return {};
    }
    const auto& partition = partlayer_to_deletes.at(state.GetPartition());
    std::set<FieldId> result;
    for (const auto& [layer_id, deletes] : partition) {
      if (layer_id < state.GetLayer()) {
        continue;
      }
      for (const auto& [delete_path, field_ids] : deletes) {
        result.insert(field_ids.begin(), field_ids.end());
      }
    }
    return std::vector<FieldId>(result.begin(), result.end());
  }
};

class EqualityDeleteApplier : public IcebergStream {
 public:
  explicit EqualityDeleteApplier(IcebergStreamPtr input, std::shared_ptr<const EqualityDeletes> equality_deletes,
                                 EqualityDeleteHandler::Config eq_del_config, std::shared_ptr<FieldIdMapper> mapper,
                                 std::shared_ptr<const IFileReaderProvider> file_reader_provider,
                                 std::shared_ptr<ILogger> logger = nullptr)
      : input_(input),
        equality_deletes_(equality_deletes),
        eq_del_config_(eq_del_config),
        field_id_mapper_(mapper),
        file_reader_provider_(file_reader_provider),
        logger_(logger) {
    Ensure(input_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": input is nullptr");
    Ensure(equality_deletes_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": equality_deletes is nullptr");
    Ensure(field_id_mapper_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": field_id_mapper is nullptr");
    Ensure(file_reader_provider_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": file_reader_provider is nullptr");
  }

  std::shared_ptr<IcebergBatch> ReadNext() override {
    auto batch = input_->ReadNext();
    if (!batch) {
      return nullptr;
    }

    if (logger_) {
      logger_->Log("", "events:equality:start_batch");
    }
    Defer defer([&]() {
      if (logger_) {
        logger_->Log("", "events:equality:end_batch");
      }
    });

    bool can_reuse_state = current_state_.has_value() && (*current_state_ == batch->GetPartition());
    if (!can_reuse_state) {
      auto open_file_lambda = [file_reader_provider = this->file_reader_provider_](const std::string& path) {
        return file_reader_provider->Open(path);
      };

      equality_delete_handler_.emplace(open_file_lambda, eq_del_config_, logger_);
      SetDeletes(batch->GetPartition());
    }
    return ApplyDeletes(batch);
  }

 private:
  std::shared_ptr<IcebergBatch> ApplyDeletes(std::shared_ptr<IcebergBatch> batch) {
    if (!batch) {
      return nullptr;
    }

    Ensure(batch->GetPartition() == *current_state_,
           std::string(__PRETTY_FUNCTION__) + ": internal error. batch_state is " +
               std::to_string(batch->GetPartition()) + ", current_state is " + std::to_string(*current_state_));

    if (equality_delete_handler_->PrepareDeletesForFile(batch->GetLayer())) {
      std::map<FieldId, std::shared_ptr<arrow::Array>> field_id_to_array = MakeFieldIdToArray(batch);
      equality_delete_handler_->PrepareDeletesForBatch(field_id_to_array);
      int32_t deleted_rows =
          batch->GetSelectionVector().EraseIf([this](int32_t row) { return equality_delete_handler_->IsDeleted(row); });
      if (logger_) {
        logger_->Log(std::to_string(deleted_rows), "metrics:equality:deleted_rows");
      }
    }

    return batch;
  }

  std::map<FieldId, std::shared_ptr<arrow::Array>> MakeFieldIdToArray(std::shared_ptr<IcebergBatch> batch) {
    Ensure(batch != nullptr, std::string(__PRETTY_FUNCTION__) + ": batch is nullptr");

    auto field_ids = equality_delete_handler_->GetEqualityDeleteFieldIds();
    std::map<FieldId, std::shared_ptr<arrow::Array>> field_id_to_array;
    for (const FieldId id : field_ids) {
      auto name = field_id_mapper_->FieldIdToColumnName(id);
      auto array = batch->GetRecordBatch()->GetColumnByName(name);
      Ensure(array != nullptr, std::string(__PRETTY_FUNCTION__) + "EqualityDeleteApplier: column " + name +
                                   " for field id " + std::to_string(id) + " is not found");

      field_id_to_array.emplace(id, array);
    }

    return field_id_to_array;
  }

  void SetDeletes(PartitionId partition) {
    current_state_ = partition;
    if (!equality_deletes_->partlayer_to_deletes.contains(partition)) {
      return;
    }
    const auto& partition_deletes = equality_deletes_->partlayer_to_deletes.at(partition);

    for (const auto& [layer_id, deletes] : partition_deletes) {
      for (const auto& del : deletes) {
        iceberg::Ensure(equality_delete_handler_->AppendDelete(del.path, del.field_ids, layer_id));
      }
    }
  }

  IcebergStreamPtr input_;
  std::shared_ptr<const EqualityDeletes> equality_deletes_;
  const EqualityDeleteHandler::Config eq_del_config_;
  std::shared_ptr<FieldIdMapper> field_id_mapper_;
  std::shared_ptr<const IFileReaderProvider> file_reader_provider_;

  std::optional<EqualityDeleteHandler> equality_delete_handler_;
  std::optional<PartitionId> current_state_;
  std::shared_ptr<ILogger> logger_;
};

}  // namespace iceberg
