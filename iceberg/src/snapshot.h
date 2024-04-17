#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>

namespace iceberg {

struct Snapshot {
  int64_t snapshot_id;
  std::optional<int64_t> parent_snapshot_id;
  int64_t sequence_number;
  int64_t timestamp_ms;
  std::string manifest_list_location;
  // at least "operation" property must be filled
  std::map<std::string, std::string> summary;
  std::optional<int64_t> schema_id;

  bool operator==(const Snapshot& other) const {
    return snapshot_id == other.snapshot_id && parent_snapshot_id == other.parent_snapshot_id &&
           sequence_number == other.sequence_number && timestamp_ms == other.timestamp_ms &&
           manifest_list_location == other.manifest_list_location && summary == other.summary &&
           schema_id == other.schema_id;
  }
};

struct SnapshotRef {
  int64_t snapshot_id;
  std::string type;
  std::optional<int32_t> min_snapshots_to_keep;
  std::optional<int64_t> max_snapshot_age_ms;
  std::optional<int64_t> max_ref_age_ms;

  bool operator==(const SnapshotRef& other) const {
    return snapshot_id == other.snapshot_id && type == other.type &&
           min_snapshots_to_keep == other.min_snapshots_to_keep && max_snapshot_age_ms == other.max_snapshot_age_ms &&
           max_ref_age_ms == other.max_ref_age_ms;
  }
};

}  // namespace iceberg
