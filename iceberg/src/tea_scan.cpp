#include "iceberg/src/tea_scan.h"

#include <iostream>
#include <memory>
#include <utility>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "iceberg/src/generated/manifest_file.hh"
#include "iceberg/src/manifest_entry.h"
#include "iceberg/src/manifest_file.h"
#include "iceberg/src/table_metadata.h"
#include "iceberg/src/tea_hive_catalog.h"

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
    return ScanMetadata{.schema = schema, .entries = {}};
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
      entries_output.emplace_back(Task{.entry = std::move(entry)});
    }
  }

  return ScanMetadata{.schema = schema, .entries = std::move(entries_output)};
}

}  // namespace iceberg::ice_tea
