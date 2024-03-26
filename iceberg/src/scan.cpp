#include "iceberg/src/scan.h"

#include <iostream>
#include <memory>
#include <utility>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "iceberg/src/hive_client.h"
#include "iceberg/src/manifest_entry.h"
#include "iceberg/src/manifest_metadata.h"
#include "iceberg/src/table_metadata.h"

namespace iceberg {

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

arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenUrl(
    const std::string& url, std::shared_ptr<arrow::fs::FileSystem> fs) {
  auto components = SplitUrl(url);
  std::string path;
  if (components.schema == "s3a" || components.schema == "s3") {
    path = components.location + components.path;
  } else {
    return ::arrow::Status::ExecutionError("unknown fs prefix for file: ", url);
  }
  return fs->OpenInputFile(path);
}

arrow::Result<std::string> ReadFile(const std::string& path,
                                    std::shared_ptr<arrow::fs::FileSystem> fs) {
  ARROW_ASSIGN_OR_RAISE(auto file, OpenUrl(path, fs));

  std::string buffer;
  ARROW_ASSIGN_OR_RAISE(auto size, file->GetSize());
  buffer.resize(size);
  ARROW_ASSIGN_OR_RAISE(auto bytes_read, file->ReadAt(0, size, buffer.data()));
  ARROW_UNUSED(bytes_read);
  return buffer;
}

}  // namespace

arrow::Result<ScanMetadata> GetScanMetadata(
    const std::string& db_name, const std::string& table_name,
    std::shared_ptr<arrow::fs::S3FileSystem> s3fs,
    const HiveClient& hive_client) {
  const std::string metadata_location =
      hive_client.GetMetadataLocation(db_name, table_name);

  ARROW_ASSIGN_OR_RAISE(const std::string table_metadata_content,
                        ReadFile(metadata_location, s3fs));
  const TableMetadataV2 table_metadata =
      MakeTableMetadataV2(table_metadata_content);
  if (!table_metadata.current_snapshot_id.has_value()) {
    return arrow::Status::ExecutionError("no current_snapshot_id");
  }

  auto maybe_manifest_list_path = table_metadata.GetCurrentManifestListPath();
  if (!maybe_manifest_list_path.has_value()) {
    return arrow::Status::ExecutionError("no manifest_list_path");
  }
  const std::string manifest_list_path = maybe_manifest_list_path.value();

  ARROW_ASSIGN_OR_RAISE(const std::string manifest_metadatas_content,
                        ReadFile(manifest_list_path, s3fs));
  const std::vector<ManifestMetadata> manifest_metadatas =
      MakeManifestList(manifest_metadatas_content);

  std::vector<ManifestEntry> entries_output;
  Schema schema = table_metadata.GetCurrentSchema();

  for (const auto& manifest_metadata : manifest_metadatas) {
    const std::string manifest_path = manifest_metadata.manifest_path;
    ARROW_ASSIGN_OR_RAISE(const std::string entries_content,
                          ReadFile(manifest_path, s3fs));
    std::vector<ManifestEntry> entries_input =
        MakeManifestEntries(entries_content);
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
      entries_output.emplace_back(std::move(entry));
    }
  }

  return ScanMetadata{.schema = std::move(schema),
                      .entries = std::move(entries_output)};
}

}  // namespace iceberg
