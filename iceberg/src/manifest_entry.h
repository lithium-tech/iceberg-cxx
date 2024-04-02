#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace iceberg {

struct DataFile {
  enum class Content {
    kData = 0,
    kPositionDelete = 1,
    kEqualityDelete = 2,
  };

  Content content;

  std::string file_path;

  std::string file_format;

  // TOOD(g.perov): partition

  int64_t record_count;

  int64_t file_size_in_bytes;

  std::optional<std::vector<std::pair<int32_t, int64_t>>> column_sizes;

  std::optional<std::vector<std::pair<int32_t, int64_t>>> value_counts;

  std::optional<std::vector<std::pair<int32_t, int64_t>>> null_value_counts;

  std::optional<std::vector<std::pair<int32_t, int64_t>>> nan_value_counts;

  std::optional<std::vector<std::pair<int32_t, int64_t>>> distinct_counts;

  std::optional<std::vector<std::pair<int32_t, std::vector<uint8_t>>>> lower_bounds;

  std::optional<std::vector<std::pair<int32_t, std::vector<uint8_t>>>> upper_bounds;

  std::optional<std::vector<uint8_t>> key_metadata;

  std::optional<std::vector<int64_t>> split_offsets;

  std::optional<std::vector<int32_t>> equality_ids;

  std::optional<int32_t> sort_order_id;
};

struct ManifestEntry {
  enum class Status {
    kExisting = 0,
    kAdded = 1,
    kDeleted = 2,
  };

  Status status;

  std::optional<int64_t> snapshot_id;

  std::optional<int64_t> sequence_number;

  std::optional<int64_t> file_sequence_number;

  DataFile data_file;
};

std::vector<ManifestEntry> MakeManifestEntries(const std::string& data);

}  // namespace iceberg
