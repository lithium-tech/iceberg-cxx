#include "iceberg/streams/iceberg/file_reader_builder.h"

#include <map>

#include "iceberg/streams/arrow/file_reader.h"
#include "iceberg/streams/arrow/projection_stream.h"

namespace iceberg {

namespace {
class StreamAwareLogger : public ILogger {
 public:
  StreamAwareLogger(std::shared_ptr<ILogger> logger, std::string stream_name)
      : logger_(logger), stream_name_(std::move(stream_name)) {
    Ensure(logger != nullptr, std::string(__PRETTY_FUNCTION__) + ": logger is nullptr");
  }

  void Log(const Message& message, const MessageType& message_type) override {
    logger_->Log(message, stream_name_ + ":" + message_type);
  }

 private:
  std::string stream_name_;
  std::shared_ptr<ILogger> logger_;
};

std::shared_ptr<StreamAwareLogger> MakeStreamAwareLogger(std::shared_ptr<ILogger> logger, std::string stream_name) {
  if (logger == nullptr) {
    return nullptr;
  }
  return std::make_shared<StreamAwareLogger>(logger, std::move(stream_name));
}

void InsertOrFail(std::unordered_map<int, int>& mp, int key, int value) {
  const bool inserted = mp.insert(std::make_pair(key, value)).second;
  Ensure(inserted, std::string(__PRETTY_FUNCTION__) + ": field ids are not unique");
}

std::unordered_map<int, int> MakeFieldIdToIndexMap(const parquet::schema::GroupNode* node,
                                                   const std::optional<SchemaNameMapper>& schema_name_mapper) {
  std::unordered_map<int, int> field_id_to_index;
  for (int i = 0; i < node->field_count(); ++i) {
    auto field = node->field(i);
    Ensure(field != nullptr, std::string(__PRETTY_FUNCTION__) + ": field is nullptr");
    auto field_id = field->field_id();

    if (field_id >= 0) {
      InsertOrFail(field_id_to_index, field_id, i);
      continue;
    }

    if (schema_name_mapper.has_value()) {
      auto field_name = field->name();
      auto possible_field_id = schema_name_mapper->GetRootMapper()->GetFieldIdByName(field_name);
      if (possible_field_id.has_value()) {
        InsertOrFail(field_id_to_index, *possible_field_id, i);
      }
    }
  }
  return field_id_to_index;
}

}  // namespace

StreamPtr<ArrowBatchWithRowPosition> FileReaderBuilder::MakeFinalStream(
    std::shared_ptr<parquet::arrow::FileReader> arrow_reader, const std::vector<int>& matching_row_groups,
    std::shared_ptr<const parquet::FileMetaData> metadata, const std::vector<int>& field_ids,
    std::shared_ptr<ILogger> logger) {
  const auto cols = GetParquetColumnInfos(*metadata, field_ids);

  std::vector<int> col_positions;
  for (const auto& col : cols) {
    if (col.column_position.has_value()) {
      col_positions.emplace_back(col.column_position.value());
    }
  }
  if (logger) {
    logger->Log(std::to_string(col_positions.size()), "metrics:data:columns_read");
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
      std::make_shared<ParquetFileReader>(arrow_reader, matching_row_groups, col_positions, logger);

  result = std::make_shared<ProjectionStream>(parquet_name_to_result_name, result);

  std::vector<std::pair<int, std::string>> remaining_field_ids_with_names;
  for (const auto& col : cols) {
    if (!col.column_position.has_value() && default_value_map_->contains(col.field_id)) {
      remaining_field_ids_with_names.emplace_back(col.field_id, col.result_name);
    }
  }

  return result = std::make_shared<DefaultValueApplier>(result, default_value_map_,
                                                        std::move(remaining_field_ids_with_names));
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

  const std::vector<int> field_ids_for_filter = [&]() {
    if (row_filter_ == nullptr) {
      return std::vector<int>{};
    }
    std::vector<int> result = row_filter_->GetInvolvedFieldIds();
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
  }();

  // names are too long
  const std::vector<int> field_ids_for_data = [](const std::vector<int>& a, const std::vector<int>& b) {
    std::vector<int> result;
    std::set_difference(a.begin(), a.end(), b.begin(), b.end(), std::back_inserter(result));
    return result;
  }(field_ids_required, field_ids_for_filter);

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

  auto data_stream = MakeFinalStream(arrow_reader, matching_row_groups, metadata, field_ids_for_data,
                                     MakeStreamAwareLogger(logger_, "data_stream"));
  auto filter_stream = MakeFinalStream(arrow_reader, matching_row_groups, metadata, field_ids_for_filter,
                                       MakeStreamAwareLogger(logger_, "filter_stream"));
  return std::make_shared<FilteringStream>(filter_stream, data_stream, row_filter_,
                                           annotated_data_path.GetPartitionLayerFile(), logger_);
}

std::vector<FileReaderBuilder::ParquetColumnInfo> FileReaderBuilder::GetParquetColumnInfos(
    const parquet::FileMetaData& metadata, const std::vector<int>& field_ids) const {
  const auto& schema = metadata.schema();
  Ensure(schema != nullptr, std::string(__PRETTY_FUNCTION__) + ": schema is nullptr");

  auto node = schema->group_node();
  Ensure(node != nullptr, std::string(__PRETTY_FUNCTION__) + ": schema->group_node() is nullptr");

  auto field_id_to_index = MakeFieldIdToIndexMap(node, schema_name_mapper_);

  std::vector<ParquetColumnInfo> column_infos;
  column_infos.reserve(field_ids.size());

  for (const auto field_id : field_ids) {
    auto it = field_id_to_index.find(field_id);
    auto result_name = mapper_->FieldIdToColumnName(field_id);
    if (it != field_id_to_index.end()) {
      // already checked node->field() != nullptr in MakeFieldIdToIndexMap
      column_infos.emplace_back(field_id, it->second, node->field(it->second)->name(), std::move(result_name));
    } else {
      column_infos.emplace_back(field_id, std::nullopt, "", std::move(result_name));
    }
  }

  return column_infos;
}

std::vector<int> FileReaderBuilder::GetRowGroupsToRetrieve(const parquet::FileMetaData& metadata,
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

}  // namespace iceberg
