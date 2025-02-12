#include "tools/common.h"

#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>

#include <algorithm>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/write.h"
#include "tools/metadata_tree.h"

namespace iceberg::tools {

void EnsureSameSchema(std::shared_ptr<iceberg::Schema> src_schema, std::shared_ptr<iceberg::Schema> dst_schema,
                      bool require_same_id, bool require_same_field_ids) {
  if (!src_schema || !dst_schema) {
    throw std::runtime_error("no schema");
  }
  const std::vector<iceberg::types::NestedField>& src_columns = src_schema->Columns();
  const std::vector<iceberg::types::NestedField>& dst_columns = dst_schema->Columns();

  if (require_same_id && src_schema->SchemaId() != dst_schema->SchemaId()) {
    throw std::runtime_error("schema mismatch: schema id");
  }

  if (src_columns.size() != dst_columns.size()) {
    throw std::runtime_error("schema mismatch: num columns");
  }

  for (size_t i = 0; i < src_columns.size(); ++i) {
    const auto& src_field = src_columns[i];
    const auto& dst_field = dst_columns[i];
    if (!src_field.type) {
      throw std::runtime_error("no type for src field " + src_field.name);
    }
    if (!dst_field.type) {
      throw std::runtime_error("no type for dst field " + dst_field.name);
    }
    if (src_field.name != dst_field.name || src_field.type->TypeId() != dst_field.type->TypeId()) {
      throw std::runtime_error("schema mismatch: " + src_field.name + " (" + src_field.type->ToString() + ") vs " +
                               dst_field.name + " (" + dst_field.type->ToString() + ")");
    }
    if (require_same_field_ids && src_field.field_id != dst_field.field_id) {
      throw std::runtime_error("schema mismatch: field_id");
    }
  }
}

void EnsureSameSortOrder(std::shared_ptr<iceberg::SortOrder> src_order, std::shared_ptr<iceberg::SortOrder> dst_order,
                         bool require_same_id, bool require_same_transform) {
  if (!src_order || !dst_order) {
    throw std::runtime_error("no sort order");
  }

  if (require_same_id && src_order->order_id != dst_order->order_id) {
    throw std::runtime_error("sort order id mismatch");
  }

  if (src_order->fields.size() != dst_order->fields.size()) {
    throw std::runtime_error("sort order mismatch: num fields");
  }

  for (size_t i = 0; i < src_order->fields.size(); ++i) {
    const auto& src_field = src_order->fields[i];
    const auto& dst_field = dst_order->fields[i];
    if (src_field.source_id != dst_field.source_id || src_field.direction != dst_field.direction ||
        src_field.null_order != dst_field.null_order) {
      throw std::runtime_error("sort order mismatch");
    }
    if (require_same_transform && src_field.transform != dst_field.transform) {
      throw std::runtime_error("sort order mismatch: transform");
    }
  }
}

iceberg::ContentFile RowGroupStats(const parquet::RowGroupMetaData& rg_meta) {
  iceberg::ContentFile out;

  for (int column = 0; column < rg_meta.num_columns(); ++column) {
    auto chunk_meta = rg_meta.ColumnChunk(column);
    if (!chunk_meta) {
      continue;
    }
    out.value_counts[column] += chunk_meta->num_values();
    out.column_sizes[column] += chunk_meta->total_compressed_size();
    if (auto chunk_stats = chunk_meta->statistics()) {
      if (chunk_stats->HasNullCount()) {
        out.null_value_counts[column] += chunk_stats->null_count();
        out.value_counts[column] += chunk_stats->null_count();
      }
      if (chunk_stats->HasDistinctCount()) {
        out.distinct_counts[column] += chunk_stats->distinct_count();
      }
#if 0
      if (chunk_stats->HasMinMax()) {
        out.lower_bounds[column] = chunk_stats->EncodeMin();
        out.upper_bounds[column] = chunk_stats->EncodeMax();
      }
#endif
    }
  }
  return out;
}

Manifest SnapshotMaker::MakeEntries(const std::filesystem::path& local_data_location,
                                    const std::filesystem::path& data_location, const std::vector<std::string>& files,
                                    iceberg::ContentFile::FileContent content) const {
  Manifest out;
  out.entries.reserve(files.size());

  out.UpdateMetadataByContent(content);

  for (auto& file_path : files) {
    uint64_t file_size = 0;
    auto file_name = std::filesystem::path(file_path).filename();
    auto parquet_meta = ParquetMetadata(fs, (local_data_location / file_name).string(), file_size);

    iceberg::ManifestEntry entry;
    entry.snapshot_id = SnapshotId();
    // entry.sequence_number not set
    // entry.file_sequence_number not set
    entry.status = iceberg::ManifestEntry::Status::kAdded;
    {
      entry.data_file.content = content;
      entry.data_file.file_path = (data_location / file_name).string();
      entry.data_file.file_size_in_bytes = file_size;
      entry.data_file.file_format = "PARQUET";
      entry.data_file.record_count = parquet_meta->num_rows();
      for (int rg = 0; rg < parquet_meta->num_row_groups(); ++rg) {
        if (auto rg_meta = parquet_meta->RowGroup(rg)) {
          AppendStats(entry.data_file, RowGroupStats(*rg_meta));
        }
      }
      entry.data_file.split_offsets = SplitOffsets(parquet_meta);
      auto sort_order = SortOrder();
      if (sort_order) {
        entry.data_file.sort_order_id = sort_order->order_id;
      }
      if (content == iceberg::ContentFile::FileContent::kEqualityDeletes) {
        if (!sort_order) {
          throw std::runtime_error("Equality delete file require equality_ids");
        }
        entry.data_file.equality_ids = sort_order->FieldIds();
      }
    }
    out.entries.emplace_back(entry);
  }
  return out;
}

iceberg::ManifestFile SnapshotMaker::MakeManifest(const std::string& path, int64_t snapshot_id, int seqno,
                                                  const std::vector<iceberg::ManifestEntry>& added_entries,
                                                  const std::vector<iceberg::ManifestEntry>& deleted_entries,
                                                  const std::vector<iceberg::ManifestEntry>& existing_entries,
                                                  iceberg::ManifestContent content, FileStats& stats) {
  iceberg::ManifestFile man;
  man.snapshot_id = snapshot_id;
  man.path = path;
  man.content = content;
  man.sequence_number = seqno;
  man.min_sequence_number = seqno;

  for (const auto& entry : added_entries) {
    stats.added.Append(entry);
  }
  for (const auto& entry : deleted_entries) {
    stats.deleted.Append(entry);
  }
  for (const auto& entry : existing_entries) {
    stats.existing.Append(entry);
  }
  man.added_files_count = stats.added.Files();
  man.added_rows_count = stats.added.Rows();
  man.deleted_files_count = stats.deleted.Files();
  man.deleted_rows_count = stats.deleted.Rows();
  man.existing_files_count = stats.existing.Files();
  man.existing_rows_count = stats.existing.Rows();
  return man;
}

std::map<std::string, std::string> SnapshotMaker::MakeSummary(const FileStats& stats) {
  // At least "operation" property must be filled:
  // append -- Only data files were added and no files were removed.
  // replace -- Data and delete files were added and removed without changing table data.
  // overwrite -- Data and delete files were added and removed in a logical overwrite operation.
  // delete -- Data files were removed and their contents logically deleted and/or delete files were added to delete
  // rows.
  std::map<std::string, std::string> summary;
  summary["operation"] = "overwrite";
  summary["added-data-files"] = std::to_string(stats.added.data_files);
  summary["added-delete-files"] = std::to_string(stats.added.DeleteFiles());
  summary["added-equality-delete-files"] = std::to_string(stats.added.eq_delete_files);
  summary["added-records"] = std::to_string(stats.added.data_rows);
  summary["added-equality-deletes"] = std::to_string(stats.added.eq_delete_rows);
  summary["added-files-size"] = std::to_string(stats.added.files_size);
  summary["total-files-size"] = std::to_string(stats.TotalFilesSize());
  summary["total-data-files"] = std::to_string(stats.TotalDataFiles());
  summary["total-delete-files"] = std::to_string(stats.TotalDeleteFiles());
  summary["total-records"] = std::to_string(stats.TotalRecords());
  summary["total-position-deletes"] = std::to_string(stats.TotalPositionDeletes());
  summary["total-equality-deletes"] = std::to_string(stats.TotalEqualityDeletes());
  return summary;
}

std::shared_ptr<iceberg::Snapshot> SnapshotMaker::MakeSnapshot(const std::string& path, const FileStats& stats) const {
  auto snap = std::make_shared<iceberg::Snapshot>();
  snap->manifest_list_location = path;
  snap->timestamp_ms = CurrentTimeMs();
  snap->snapshot_id = SnapshotId();
  snap->parent_snapshot_id = parent_snap->snapshot_id;
  snap->schema_id = parent_snap->schema_id;
  snap->sequence_number = parent_snap->sequence_number + 1;
  snap->summary = MakeSummary(stats);
  return snap;
}

SnapshotMaker::SnapshotMaker(std::shared_ptr<arrow::fs::FileSystem> fs_,
                             const std::shared_ptr<iceberg::TableMetadataV2>& prev_table_metadata,
                             int64_t current_time_ms, int meta_seqno)
    : fs(fs_) {
  table_metadata = std::make_shared<iceberg::TableMetadataV2>(*prev_table_metadata);
  table_metadata->last_sequence_number += 1;
  if (meta_seqno) {
    table_metadata->last_sequence_number = meta_seqno;
  }
  table_metadata->last_updated_ms = current_time_ms;
  std::optional<int64_t> parent_snapshot_id = table_metadata->current_snapshot_id;
  if (parent_snapshot_id) {
    table_metadata->current_snapshot_id = *parent_snapshot_id + 1;
  } else {
    table_metadata->current_snapshot_id = 1;
    parent_snap = std::make_shared<iceberg::Snapshot>();
  }

  for (auto& snap : table_metadata->snapshots) {
    if (snap->snapshot_id == parent_snapshot_id) {
      parent_snap = snap;
      break;
    }
  }
  if (parent_snapshot_id && !parent_snap) {
    throw std::runtime_error("no snapshot for snapshot_id " + std::to_string(*parent_snapshot_id));
  }
}

void SnapshotMaker::MakeMetadataFiles(const std::filesystem::path& out_metadata_location,
                                      const std::filesystem::path& remote_location,
                                      const std::unordered_map<std::string, std::shared_ptr<Manifest>>& existing,
                                      const Manifest& added_data_entries, const Manifest& added_delete_entries,
                                      int64_t schema_id, const std::optional<std::string>& result_metadata_path) {
  std::vector<iceberg::ManifestEntry> existing_data_entries;
  std::vector<iceberg::ManifestEntry> existing_delete_entries;
  for (auto& [name, man] : existing) {
    for (auto& entry : man->entries) {
      switch (entry.status) {
        case iceberg::ManifestEntry::Status::kAdded:
        case iceberg::ManifestEntry::Status::kExisting:
          if (entry.data_file.content == iceberg::ContentFile::FileContent::kData) {
            existing_data_entries.push_back(entry);
            existing_data_entries.back().status = iceberg::ManifestEntry::Status::kExisting;
          } else if (entry.data_file.content == iceberg::ContentFile::FileContent::kEqualityDeletes) {
            existing_delete_entries.push_back(entry);
            existing_delete_entries.back().status = iceberg::ManifestEntry::Status::kExisting;
          } else {
            throw std::runtime_error("positional deletes are not supported: " + name);
          }
          break;
        case iceberg::ManifestEntry::Status::kDeleted:
          break;
      }
    }
  }

  FileStats parent_stats;
  parent_stats.existing = FileCounts::FromSummary(parent_snap->summary);
  FileStats data_stats;
  FileStats delete_stats;

  std::string data_manifest_name = name_maker.ManifestName(name_maker.uuid_added_data_manifest, 0);
  std::string delete_manifest_name = name_maker.ManifestName(name_maker.uuid_added_delete_manifest, 1);

  std::vector<iceberg::ManifestFile> manifest_list = {
      MakeDataManifest(remote_location / data_manifest_name, added_data_entries.entries, {}, existing_data_entries,
                       data_stats),
      MakeEqDeleteManifest(remote_location / delete_manifest_name, added_delete_entries.entries, {},
                           existing_delete_entries, delete_stats)};

  FileStats total_stats = parent_stats + data_stats + delete_stats;

  auto snapshot_name = MakeSnapshotName();
  std::shared_ptr<iceberg::Snapshot> snap = MakeSnapshot(remote_location / snapshot_name, total_stats);
  snap->schema_id = schema_id;

  // table_metadata->properties
  table_metadata->refs["main"].snapshot_id = SnapshotId();
  table_metadata->refs["main"].type = "branch";
  table_metadata->snapshots.push_back(snap);

  table_metadata->snapshot_log.push_back(
      iceberg::SnapshotLog{.timestamp_ms = CurrentTimeMs(), .snapshot_id = SnapshotId()});
  table_metadata->metadata_log.push_back(
      iceberg::MetadataLog{.timestamp_ms = CurrentTimeMs(), .metadata_file = table_metadata->location});

  auto metadata_filename = result_metadata_path.has_value()
                               ? result_metadata_path.value()
                               : static_cast<std::string>(std::filesystem::path(table_metadata->location).filename());
  iceberg::ice_tea::WriteManifestRemote(fs, out_metadata_location / data_manifest_name, added_data_entries);
  iceberg::ice_tea::WriteManifestRemote(fs, out_metadata_location / delete_manifest_name, added_delete_entries);
  iceberg::ice_tea::WriteManifestListRemote(fs, out_metadata_location / snapshot_name, manifest_list);
  iceberg::ice_tea::WriteMetadataFileRemote(fs, out_metadata_location / metadata_filename, table_metadata);
}

}  // namespace iceberg::tools
