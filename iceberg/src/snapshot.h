#pragma once

namespace iceberg {

struct Snapshot {
  int64_t snapshot_id;
  std::optional<int64_t> parent_snapshot_id;
  int64_t sequence_number;
  int64_t timestamp_ms;
  std::string manifest_list;
  // at least "operation" property must be filled
  std::map<std::string, std::string> summary;
  std::optional<int64_t> schema_id;
};

}  // namespace iceberg
