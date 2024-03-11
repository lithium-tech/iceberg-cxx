#pragma once

#include <string>
#include <vector>

namespace iceberg {

enum class ContentType {
  kData = 0,
  kDelete = 1,
};

struct ManifestMetadata {
  // Location URI with FS scheme
  std::string manifest_path;
  // Total file size in bytes
  int64_t manifest_length;
  // Spec ID used to write
  int32_t partition_spec_id;
  // Contents of the manifest: 0=data, 1=deletes
  ContentType content_type;
  // Sequence number when the manifest was added
  int64_t sequence_number;
  // Lowest sequence number in the manifest
  int64_t min_sequence_number;
  // Snapshot ID that added the manifest
  int64_t added_snapshot_id;
  // Added entry count
  int32_t added_data_files_count;
  // Existing entry count
  int32_t existing_data_files_count;
  // Deleted entry count
  int32_t deleted_data_files_count;
  // Added rows count
  int64_t added_rows_count;
  // Existing rows count
  int64_t existing_rows_count;
  // Deleted rows count
  int64_t deleted_rows_count;
  // TODO(gmusya): partitions
  // TODO(gmusya): key metadata
};

std::vector<ManifestMetadata> MakeManifestList(const std::string& data);

}  // namespace iceberg
