#include "iceberg/streams/iceberg/file_reader_builder.h"

namespace iceberg {

namespace {

void InsertOrFail(std::unordered_set<int>& set, int x) {
  Ensure(!set.contains(x), "Field ids are not unique");
  set.insert(x);
}

std::unordered_map<int, int> MakeIndexToFieldIdMap(const parquet::schema::GroupNode* node,
                                                   const std::optional<std::string>& schema_name_mapping) {
  if (!schema_name_mapping.has_value()) {
    return {};
  }
  std::unordered_set<int> field_ids_set;
  std::optional<SchemaNameMapper> schema_name_mapper;
  std::optional<SchemaNameMapper::Node> root_node;
  std::unordered_map<int, int> index_to_field_id;
  for (int i = 0; i < node->field_count(); ++i) {
    if (node->field(i)->field_id() >= 0) {
      InsertOrFail(field_ids_set, node->field(i)->field_id());
      continue;
    }
    if (!schema_name_mapper.has_value()) {
      schema_name_mapper = SchemaNameMapper(*schema_name_mapping);
      root_node = schema_name_mapper.value().GetRootNode();
    }
    auto field_name = node->field(i)->name();
    auto field_id = root_node->GetFieldIdByName(field_name);
    if (field_id.has_value()) {
      index_to_field_id[i] = field_id.value();
      InsertOrFail(field_ids_set, field_id.value());
    }
  }
  return index_to_field_id;
}

}  // namespace

std::vector<FileReaderBuilder::ParquetColumnInfo> FileReaderBuilder::GetColumnPositionsToRetrieveByFieldIds(
    const parquet::FileMetaData& metadata, const std::vector<int>& field_ids) const {
  const auto& schema = metadata.schema();
  Ensure(schema != nullptr, std::string(__PRETTY_FUNCTION__) + ": schema is nullptr");

  auto node = schema->group_node();

  GroupNodeWrapper group_node_wrapper(node, MakeIndexToFieldIdMap(node, schema_name_mapping_));

  const auto field_count = group_node_wrapper.GetFieldCount();

  std::vector<ParquetColumnInfo> column_infos;

  for (const auto& field_id_to_find : field_ids) {
    bool found = false;

    for (int i = 0; i < field_count; ++i) {
      if (field_id_to_find == group_node_wrapper.GetRealFieldId(i)) {
        found = true;
        auto result_name = mapper_->FieldIdToColumnName(field_id_to_find);
        column_infos.emplace_back(field_id_to_find, i, group_node_wrapper.GetName(i), std::move(result_name));
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

FileReaderBuilder::GroupNodeWrapper::GroupNodeWrapper(const parquet::schema::GroupNode* node,
                                                      std::unordered_map<int, int> index_to_field_id)
    : node_(node), index_to_field_id_(std::move(index_to_field_id)) {
  Ensure(node_, std::string(__PRETTY_FUNCTION__) + ": group_node is nullptr");
  for (int i = 0; i < GetFieldCount(); ++i) {
    Ensure(node_->field(i) != nullptr, std::string(__PRETTY_FUNCTION__) + ": field is nullptr");
  }
}

int FileReaderBuilder::GroupNodeWrapper::GetRealFieldId(int index) const {
  auto it = index_to_field_id_.find(index);
  if (it == index_to_field_id_.end()) {
    return node_->field(index)->field_id();
  }
  return it->second;
}

int FileReaderBuilder::GroupNodeWrapper::GetFieldCount() const { return node_->field_count(); }

std::string FileReaderBuilder::GroupNodeWrapper::GetName(int index) const { return node_->field(index)->name(); }

}  // namespace iceberg
