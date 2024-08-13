#pragma once

#include <string>
#include <vector>

namespace iceberg {

enum class ManifestContent {
  kData = 0,
  kDeletes = 1,
};

struct PartitionFieldSummary {
  bool contains_null;
  std::optional<bool> contains_nan;
  std::vector<uint8_t> lower_bound;
  std::vector<uint8_t> upper_bound;
};

// see also https://iceberg.apache.org/javadoc/1.5.0/org/apache/iceberg/ManifestFile.html
struct ManifestFile {
  int32_t added_files_count{};
  int64_t added_rows_count{};
  ManifestContent content = ManifestContent::kData;
  int32_t deleted_files_count{};
  int64_t deleted_rows_count{};
  int32_t existing_files_count{};
  int64_t existing_rows_count{};
  int64_t length{};
  int64_t min_sequence_number{};
  int32_t partition_spec_id{};
  std::string path{};
  int64_t sequence_number{};
  int64_t snapshot_id{};
  std::vector<PartitionFieldSummary> partitions;

  // TODO(gmusya): key metadata
};

namespace ice_tea {

// SerDe for manifest list
std::vector<ManifestFile> ReadManifestList(std::istream& istream);
std::string WriteManifestList(const std::vector<ManifestFile>& manifest_list);

}  // namespace ice_tea
}  // namespace iceberg
