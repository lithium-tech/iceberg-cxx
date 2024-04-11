#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace iceberg {

struct ContentFile {
  enum class FileContent {
    kData = 0,
    kPositionDeletes = 1,
    kEqualityDeletes = 2,
  };

  FileContent content;
  std::string file_path;
  std::string file_format;

  // TOOD(g.perov): partition

  int64_t record_count;
  int64_t file_size_in_bytes;
  std::map<int32_t, int64_t> column_sizes;
  std::map<int32_t, int64_t> value_counts;
  std::map<int32_t, int64_t> null_value_counts;
  std::map<int32_t, int64_t> nan_value_counts;
  std::map<int32_t, int64_t> distinct_counts;
  std::map<int32_t, std::vector<uint8_t>> lower_bounds;
  std::map<int32_t, std::vector<uint8_t>> upper_bounds;
  std::vector<uint8_t> key_metadata;
  std::vector<int64_t> split_offsets;
  std::vector<int32_t> equality_ids;
  std::optional<int32_t> sort_order_id;
};

struct DataFile : public ContentFile {
  DataFile() = default;
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
