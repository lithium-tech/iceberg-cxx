#pragma once

#include <string>
#include <vector>

namespace iceberg {

enum class ManifestContent {
  kData = 0,
  kDeletes = 1,
};

// see also https://iceberg.apache.org/javadoc/1.5.0/org/apache/iceberg/ManifestFile.html
struct ManifestFile {
  int32_t added_files_count;
  int64_t added_rows_count;
  ManifestContent content;
  int32_t deleted_files_count;
  int64_t deleted_rows_count;
  int32_t existing_files_count;
  int64_t existing_rows_count;
  int64_t length;
  int64_t min_sequence_number;
  int32_t partition_spec_id;
  std::string path;
  int64_t sequence_number;
  int64_t snapshot_id;

  // TODO(gmusya): partitions
  // TODO(gmusya): key metadata
};

std::vector<ManifestFile> MakeManifestList(const std::string& data);

}  // namespace iceberg
