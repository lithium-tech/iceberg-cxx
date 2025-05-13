#pragma once

#include <arrow/status.h>
#include <iceberg/common/rg_metadata.h>
#include <parquet/metadata.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/common/batch.h"
#include "iceberg/common/fs/file_reader_provider.h"
#include "iceberg/common/logger.h"
#include "iceberg/common/rg_metadata.h"
#include "iceberg/common/selection_vector.h"
#include "iceberg/streams/arrow/file_reader.h"
#include "iceberg/streams/arrow/projection_stream.h"
#include "iceberg/streams/iceberg/data_entries_meta_stream.h"
#include "iceberg/streams/iceberg/data_scan.h"
#include "iceberg/streams/iceberg/equality_delete_applier.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"
#include "iceberg/streams/iceberg/mapper.h"
#include "iceberg/streams/iceberg/plan.h"
#include "iceberg/streams/iceberg/row_group_filter.h"
#include "parquet/arrow/reader.h"
#include "parquet/type_fwd.h"

namespace iceberg {

class ArrowStreamWrapper : public IcebergStream {
 public:
  ArrowStreamWrapper(StreamPtr<ArrowBatchWithRowPosition> input, PartitionLayerFile state)
      : input_(input), state_(std::move(state)) {}

  std::shared_ptr<IcebergBatch> ReadNext() override {
    auto batch = input_->ReadNext();
    if (!batch) {
      return nullptr;
    }
    SelectionVector<int32_t> selection_vector(batch->GetRecordBatch()->num_rows());

    BatchWithSelectionVector batch_with_vec(batch->GetRecordBatch(), std::move(selection_vector));
    PartitionLayerFilePosition result_state(state_, batch->row_position);

    return std::make_shared<IcebergBatch>(std::move(batch_with_vec), std::move(result_state));
  }

 private:
  StreamPtr<ArrowBatchWithRowPosition> input_;

  const PartitionLayerFile state_;
};

// TODO(gmusya): maybe extract some private methods into functions
class FileReaderBuilder : public DataScanner::IIcebergStreamBuilder {
 public:
  FileReaderBuilder(std::vector<int> field_ids_to_retrieve, std::shared_ptr<const EqualityDeletes> equality_deletes,
                    std::shared_ptr<const FieldIdMapper> mapper,
                    std::shared_ptr<const IFileReaderProvider> file_reader_provider,
                    std::shared_ptr<const IRowGroupFilter> rg_filter, std::shared_ptr<ILogger> logger = nullptr)
      : field_ids_to_retrieve_(std::move(field_ids_to_retrieve)),
        equality_deletes_(equality_deletes),
        mapper_(mapper),
        file_reader_provider_(file_reader_provider),
        rg_filter_(rg_filter),
        logger_(logger) {
    Ensure(equality_deletes_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": equality_deletes is nullptr");
    Ensure(mapper_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": mapper is nullptr");
    Ensure(file_reader_provider_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": file_reader_provider is nullptr");
  }

  IcebergStreamPtr Build(const AnnotatedDataPath& annotated_data_path) override {
    auto arrow_reader = MakeArrowReader(annotated_data_path.GetPath());
    Ensure(arrow_reader != nullptr, std::string(__PRETTY_FUNCTION__) + ": arrow_reader is nullptr");

    if (logger_) {
      logger_->Log(std::to_string(1), "metrics:data:files_read");
    }

    auto parquet_reader = arrow_reader->parquet_reader();
    Ensure(parquet_reader != nullptr, std::string(__PRETTY_FUNCTION__) + ": parquet_reader is nullptr");

    std::shared_ptr<const parquet::FileMetaData> metadata = parquet_reader->metadata();
    Ensure(metadata != nullptr, std::string(__PRETTY_FUNCTION__) + ": metadata is nullptr");

    const std::vector<int> field_ids_required = [&]() {
      auto field_ids_for_equality_delete = equality_deletes_->GetFieldIds(annotated_data_path.GetPartitionLayer());

      if (logger_) {
        logger_->Log(std::to_string(field_ids_for_equality_delete.size()), "metrics:data:columns_equality_delete");
      }

      std::vector<int> result = field_ids_to_retrieve_;
      for (const int f : field_ids_for_equality_delete) {
        result.emplace_back(f);
      }

      std::sort(result.begin(), result.end());
      result.erase(std::unique(result.begin(), result.end()), result.end());

      if (logger_) {
        logger_->Log(std::to_string(static_cast<int>(result.size()) - static_cast<int>(field_ids_to_retrieve_.size())),
                     "metrics:data:columns_only_equality_delete");
      }

      return result;
    }();

    const auto cols = GetColumnPositionsToRetrieveByFieldIds(*metadata, field_ids_required);

    std::vector<int> col_positions = [&]() {
      std::vector<int> result;
      for (const auto& col : cols) {
        if (col.column_position.has_value()) {
          result.emplace_back(col.column_position.value());
        }
      }
      return result;
    }();

    const std::vector<int> row_groups_matching_offsets =
        GetRowGroupsToRetrieve(*metadata, annotated_data_path.GetSegments());
    int64_t planned_row_groups = row_groups_matching_offsets.size();
    int64_t skipped_row_groups = 0;

    const std::vector<int> matching_row_groups = [&]() {
      if (!rg_filter_) {
        return row_groups_matching_offsets;
      }

      std::vector<int> result;
      for (int rg_ind : row_groups_matching_offsets) {
        std::shared_ptr<const parquet::RowGroupMetaData> rg_meta = metadata->RowGroup(rg_ind);
        Ensure(rg_meta != nullptr, std::string(__PRETTY_FUNCTION__) + ": rg_meta is nullptr");

        if (!rg_filter_->CanSkipRowGroup(*rg_meta)) {
          result.emplace_back(rg_ind);
        } else {
          ++skipped_row_groups;
        }
      }

      return result;
    }();

    if (logger_) {
      logger_->Log(std::to_string(planned_row_groups), "metrics:row_groups:planned");
      logger_->Log(std::to_string(skipped_row_groups), "metrics:row_groups:skipped");

      logger_->Log(std::to_string(col_positions.size()), "metrics:data:columns_read");
    }

    StreamPtr<ArrowBatchWithRowPosition> result = std::make_shared<ParquetFileReader>(
        std::shared_ptr<parquet::arrow::FileReader>(arrow_reader), matching_row_groups, col_positions, logger_);

    const std::map<std::string, std::string> parquet_name_to_result_name = [&]() {
      std::map<std::string, std::string> result;
      for (const auto& col : cols) {
        if (col.column_position.has_value()) {
          result[col.parquet_name] = col.result_name;
        }
      }

      return result;
    }();

    result = std::make_shared<ProjectionStream>(parquet_name_to_result_name, result);

    return std::make_shared<ArrowStreamWrapper>(result, annotated_data_path.GetPartitionLayerFile());
  }

 private:
  std::shared_ptr<parquet::arrow::FileReader> MakeArrowReader(const std::string& path) {
    auto input_file = iceberg::ValueSafe(file_reader_provider_->Open(path));
    return input_file;
  }

  struct ParquetColumnInfo {
    int field_id;

    std::optional<int> column_position;
    std::string parquet_name;

    std::string result_name;

    ParquetColumnInfo() = delete;
    ParquetColumnInfo(int f, std::optional<int> pos, std::string par_name, std::string res_name)
        : field_id(f), column_position(pos), parquet_name(std::move(par_name)), result_name(std::move(res_name)) {}
  };

  std::vector<ParquetColumnInfo> GetColumnPositionsToRetrieveByFieldIds(const parquet::FileMetaData& metadata,
                                                                        const std::vector<int>& field_ids) const {
    const auto& schema = metadata.schema();
    Ensure(schema != nullptr, std::string(__PRETTY_FUNCTION__) + ": schema is nullptr");

    const auto& group_node = schema->group_node();
    Ensure(group_node != nullptr, std::string(__PRETTY_FUNCTION__) + ": group_node is nullptr");

    const auto field_count = group_node->field_count();

    std::vector<ParquetColumnInfo> column_infos;

    for (const auto& field_id_to_find : field_ids) {
      bool found = false;

      for (int i = 0; i < field_count; ++i) {
        auto field = group_node->field(i);
        Ensure(field != nullptr, std::string(__PRETTY_FUNCTION__) + ": field is nullptr");

        const int current_field_id = field->field_id();
        if (field_id_to_find == current_field_id) {
          found = true;
          auto result_name = mapper_->FieldIdToColumnName(field_id_to_find);
          column_infos.emplace_back(field_id_to_find, i, field->name(), std::move(result_name));
          break;
        }
      }

      if (!found) {
        auto result_name = mapper_->FieldIdToColumnName(field_id_to_find);
        column_infos.emplace_back(field_id_to_find, std::nullopt, "", std::move(result_name));
      }
    }

    return column_infos;
  }

  static std::vector<int> GetRowGroupsToRetrieve(const parquet::FileMetaData& metadata,
                                                 const std::vector<AnnotatedDataPath::Segment>& segments) {
    std::vector<int> row_groups_to_process;

    size_t last_not_mapped_part_id = 0;
    for (int i = 0; i < metadata.num_row_groups(); ++i) {
      auto rg_meta = metadata.RowGroup(i);
      Ensure(rg_meta != nullptr, std::string(__PRETTY_FUNCTION__) + ": rg_meta is nullptr");

      int64_t file_offset = RowGroupMetaToFileOffset(*rg_meta);
      while (last_not_mapped_part_id < segments.size()) {
        const auto seg_length = segments[last_not_mapped_part_id].length;  // 0 <=> to end
        const auto& seg_start = segments[last_not_mapped_part_id].offset;
        const auto& seg_end = segments[last_not_mapped_part_id].offset + segments[last_not_mapped_part_id].length;

        if (seg_start <= file_offset && (file_offset < seg_end || seg_length == 0)) {
          row_groups_to_process.emplace_back(i);
          break;
        }
        if (file_offset >= seg_end) {
          ++last_not_mapped_part_id;
          continue;
        }
        break;
      }
    }
    return row_groups_to_process;
  }

  const std::vector<ColumnInfo> columns_to_retrieve_;
  const std::vector<int> field_ids_to_retrieve_;
  std::shared_ptr<const EqualityDeletes> equality_deletes_;
  std::shared_ptr<const FieldIdMapper> mapper_;
  std::shared_ptr<const IFileReaderProvider> file_reader_provider_;
  std::shared_ptr<const IRowGroupFilter> rg_filter_;
  std::shared_ptr<ILogger> logger_;
};

}  // namespace iceberg
