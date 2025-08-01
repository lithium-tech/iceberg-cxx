#include "iceberg/streams/iceberg/file_reader_builder.h"

namespace iceberg {

namespace {

void InsertOrFail(std::unordered_map<int, int>& mp, int key, int value) {
  Ensure(mp.insert(std::make_pair(key, value)).second, std::string(__PRETTY_FUNCTION__) + ": field ids are not unique");
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

    if (!schema_name_mapper.has_value()) continue;
    auto field_name = field->name();
    auto possible_field_id = schema_name_mapper->GetRootMapper().GetFieldIdByName(field_name);
    if (possible_field_id.has_value()) {
      InsertOrFail(field_id_to_index, *possible_field_id, i);
    }
  }
  return field_id_to_index;
}

}  // namespace

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
