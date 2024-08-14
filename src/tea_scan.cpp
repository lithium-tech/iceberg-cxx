#include "src/tea_scan.h"

#include <iostream>
#include <memory>
#include <utility>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "src/generated/manifest_file.hh"
#include "src/manifest_entry.h"
#include "src/manifest_file.h"
#include "src/table_metadata.h"
#include "src/tea_column_stats.h"
#include "src/tea_hive_catalog.h"

namespace iceberg::ice_tea {

namespace {

struct UrlComponents {
  std::string schema;
  std::string location;
  std::string path;
};

UrlComponents SplitUrl(const std::string& url) {
  UrlComponents result;
  const auto schema_delimiter = std::string("://");
  auto delimiter_pos = url.find(schema_delimiter);
  std::string::size_type location_pos;
  if (delimiter_pos == std::string::npos) {
    location_pos = 0;
  } else {
    result.schema.assign(url, 0, delimiter_pos);
    location_pos = delimiter_pos + schema_delimiter.size();
  }
  auto non_location_pos = url.find_first_of("/?#", location_pos);
  result.location.assign(url, location_pos, non_location_pos - location_pos);
  if (non_location_pos != std::string::npos && url[non_location_pos] == '/') {
    auto non_path_pos = url.find_first_of("?#", non_location_pos + 1);
    result.path.assign(url, non_location_pos, non_path_pos - non_location_pos);
  }
  return result;
}

bool IsKnownPrefix(const std::string& prefix) { return prefix == "s3a" || prefix == "s3"; }

std::string UrlToPath(const std::string& url) {
  auto components = SplitUrl(url);
  if (IsKnownPrefix(components.schema)) {
    return components.location + components.path;
  }
  return {};
}

}  // namespace

std::optional<int64_t> Task::GetValueCounts(int32_t field_id) const {
  for (const auto& [id, cnt] : entry.data_file.value_counts) {
    if (id == field_id) {
      return cnt;
    }
  }
  return std::nullopt;
}

std::optional<int64_t> Task::GetColumnSize(int32_t field_id) const {
  for (const auto& [id, cnt] : entry.data_file.column_sizes) {
    if (id == field_id) {
      return cnt;
    }
  }
  return std::nullopt;
}

ColumnStats Task::GetColumnStats(int32_t field_id) const {
  uint64_t total_rows = entry.data_file.record_count;
  std::optional<int64_t> value_count = GetValueCounts(field_id);
  std::optional<int64_t> column_size = GetColumnSize(field_id);

  ColumnStats result{.null_count = -1,
                     .distinct_count = -1,
                     .not_null_count = -1,
                     .total_compressed_size = -1,
                     .total_uncompressed_size = -1};

  if (value_count.has_value()) {
    result.not_null_count = value_count.value();
    result.null_count = total_rows - value_count.value();
  }

  if (column_size.has_value()) {
    result.total_compressed_size = column_size.value();
    // TODO(gmusya): estimate total_uncompressed_size
  }

  return result;
}

namespace {
void Add(ColumnStats& base, const ColumnStats& addition) {
  base.distinct_count = -1;

  if (addition.not_null_count == -1 || base.not_null_count == -1) {
    base.not_null_count = -1;
  } else {
    base.not_null_count += addition.not_null_count;
  }

  if (addition.null_count == -1 || base.null_count == -1) {
    base.null_count = -1;
  } else {
    base.null_count += addition.null_count;
  }

  if (addition.total_uncompressed_size == -1 || base.total_uncompressed_size == -1) {
    base.total_uncompressed_size = -1;
  } else {
    base.total_uncompressed_size += addition.total_uncompressed_size;
  }

  if (addition.total_compressed_size == -1 || base.total_compressed_size == -1) {
    base.total_compressed_size = -1;
  } else {
    base.total_compressed_size += addition.total_compressed_size;
  }
}
}  // namespace

arrow::Result<ColumnStats> ScanMetadata::GetColumnStats(const std::string& column_name) const {
  int field_id = -1;
  {
    auto maybe_field_id = schema->FindColumnIgnoreCase(column_name);
    if (!maybe_field_id.has_value()) {
      return arrow::Status::ExecutionError("GetIcebergColumnStats: Column ", column_name, " not found in schema");
    }
    field_id = maybe_field_id.value();
  }

  ColumnStats result{.null_count = 0,
                     .distinct_count = 0,
                     .not_null_count = 0,
                     .total_compressed_size = 0,
                     .total_uncompressed_size = 0};

  for (const auto& task : data_entries) {
    if (task.entry.data_file.content != DataFile::FileContent::kData) {
      continue;
    }
    ColumnStats task_stats = task.GetColumnStats(field_id);
    Add(result, task_stats);
  }

  return result;
}

arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenFile(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                                     const std::string& url) {
  auto path = UrlToPath(url);
  if (path.empty()) {
    return ::arrow::Status::ExecutionError("bad url: ", url);
  }
  return fs->OpenInputFile(path);
}

arrow::Result<std::string> ReadFile(std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& url) {
  ARROW_ASSIGN_OR_RAISE(auto file, OpenFile(fs, url));

  std::string buffer;
  ARROW_ASSIGN_OR_RAISE(auto size, file->GetSize());
  buffer.resize(size);
  ARROW_ASSIGN_OR_RAISE(auto bytes_read, file->ReadAt(0, size, buffer.data()));
  ARROW_UNUSED(bytes_read);
  return buffer;
}

arrow::Result<ScanMetadata> GetScanMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                            const std::string& metadata_location) {
  ARROW_ASSIGN_OR_RAISE(const std::string table_metadata_content, ReadFile(fs, metadata_location));
  auto table_metadata = ice_tea::ReadTableMetadataV2(table_metadata_content);
  if (!table_metadata) {
    return arrow::Status::ExecutionError("cannot read metadata");
  }
  if (!table_metadata->current_snapshot_id.has_value()) {
    return arrow::Status::ExecutionError("no current_snapshot_id");
  }

  std::shared_ptr<Schema> schema = table_metadata->GetCurrentSchema();
  if (table_metadata->snapshots.empty()) {
    return ScanMetadata{.schema = schema};
  }

  auto maybe_manifest_list_path = table_metadata->GetCurrentManifestListPath();
  if (!maybe_manifest_list_path.has_value()) {
    return arrow::Status::ExecutionError("no manifest_list_path");
  }
  const std::string manifest_list_path = maybe_manifest_list_path.value();

  ARROW_ASSIGN_OR_RAISE(const std::string manifest_metadatas_content, ReadFile(fs, manifest_list_path));

  std::stringstream ss(manifest_metadatas_content);
  const std::vector<ManifestFile> manifest_metadatas = ice_tea::ReadManifestList(ss);

  std::vector<Task> entries_output;

  ScanMetadata result;
  result.schema = schema;

  for (const auto& manifest_metadata : manifest_metadatas) {
    const std::string manifest_path = manifest_metadata.path;
    ARROW_ASSIGN_OR_RAISE(const std::string entries_content, ReadFile(fs, manifest_path));
    std::vector<ManifestEntry> entries_input = ice_tea::ReadManifestEntries(entries_content);
    for (auto&& entry : entries_input) {
      if (entry.status == ManifestEntry::Status::kDeleted) {
        continue;
      }

      int64_t data_sequence_number;
      if (entry.sequence_number.has_value()) {
        data_sequence_number = entry.sequence_number.value();
      } else if (entry.status == ManifestEntry::Status::kAdded) {
        data_sequence_number = manifest_metadata.sequence_number;
      } else {
        return arrow::Status::ExecutionError("no sequence_number");
      }
      entry.sequence_number = data_sequence_number;

      switch (entry.data_file.content) {
        case ContentFile::FileContent::kData:
          result.data_entries.emplace_back(Task{.entry = std::move(entry)});
          break;
        case ContentFile::FileContent::kPositionDeletes:
          result.positional_delete_entries.emplace_back(std::move(entry));
          break;
        case ContentFile::FileContent::kEqualityDeletes:
          result.equality_delete_entries.emplace_back(std::move(entry));
          break;
      }
    }
  }

  for (auto& task : result.data_entries) {
    auto& task_entry = task.entry;
    /*
    A position delete file must be applied to a data file when all of the following are true:
    - The data file's data sequence number is less than or equal to the delete file's data sequence number
    - The data file's partition (both spec and partition values) is equal to the delete file's partition
    */
    for (size_t i = 0; i < result.positional_delete_entries.size(); ++i) {
      const auto& pos_entry = result.positional_delete_entries[i];
      if (pos_entry.sequence_number >= task_entry.sequence_number) {
        task.pos_del_ids.emplace_back(i);
      }
    }

    /*
    An equality delete file must be applied to a data file when all of the following are true:
    - The data file's data sequence number is strictly less than the delete's data sequence number
    - The data file's partition (both spec id and partition values) is equal to the delete file's partition or the
    delete file's partition spec is unpartitioned
    */
    for (size_t i = 0; i < result.equality_delete_entries.size(); ++i) {
      const auto& eq_entry = result.equality_delete_entries[i];
      if (eq_entry.sequence_number > task_entry.sequence_number) {
        task.eq_del_ids.emplace_back(i);
      }
    }
  }

  return result;
}

}  // namespace iceberg::ice_tea
