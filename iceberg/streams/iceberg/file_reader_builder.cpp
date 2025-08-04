#include "iceberg/streams/iceberg/file_reader_builder.h"

#include <map>

#include "iceberg/streams/arrow/file_reader.h"
#include "iceberg/streams/arrow/projection_stream.h"

namespace iceberg {

StreamPtr<ArrowBatchWithRowPosition> FileReaderBuilder::MakeFinalStream(
    std::shared_ptr<parquet::arrow::FileReader> arrow_reader, const std::vector<int>& matching_row_groups,
    std::shared_ptr<const parquet::FileMetaData> metadata, const std::vector<int>& field_ids) {
  const auto cols = GetColumnPositionsToRetrieveByFieldIds(*metadata, field_ids);

  std::vector<int> col_positions;
  for (const auto& col : cols) {
    if (col.column_position.has_value()) {
      col_positions.emplace_back(col.column_position.value());
    }
  }
  if (logger_) {
    logger_->Log(std::to_string(col_positions.size()), "metrics:data:columns_read");
  }

  const std::map<std::string, std::string> parquet_name_to_result_name = [&]() {
    std::map<std::string, std::string> result;
    for (const auto& col : cols) {
      if (col.column_position.has_value()) {
        result[col.parquet_name] = col.result_name;
      }
    }

    return result;
  }();

  StreamPtr<ArrowBatchWithRowPosition> result =
      std::make_shared<ParquetFileReader>(arrow_reader, matching_row_groups, col_positions, logger_);

  return std::make_shared<ProjectionStream>(parquet_name_to_result_name, result);
}

IcebergStreamPtr FileReaderBuilder::Build(const AnnotatedDataPath& annotated_data_path) {
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
  }

  auto result = MakeFinalStream(arrow_reader, matching_row_groups, metadata, field_ids_required);

  return std::make_shared<ArrowStreamWrapper>(result, annotated_data_path.GetPartitionLayerFile());
}

}  // namespace iceberg
