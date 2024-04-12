#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/src/schema.h"
#include "iceberg/src/snapshot.h"

namespace iceberg {

struct TableMetadataV2 {
  TableMetadataV2(std::string table_uuid, std::string location, int64_t last_sequence_number, int64_t last_updated_ms,
                  int32_t last_column_id, std::vector<std::shared_ptr<Schema>>&& schemas, int32_t current_schema_id,
                  int32_t default_spec_id, int32_t last_partition_id, std::map<std::string, std::string>&& properties,
                  std::optional<int64_t> current_snapshot_id, std::vector<std::shared_ptr<Snapshot>>&& snapshots,
                  std::optional<std::vector<std::pair<int64_t, int64_t>>> snapshot_log,
                  std::optional<std::vector<std::pair<int64_t, std::string>>> metadata_log,
                  int32_t default_sort_order_id)
      : table_uuid(std::move(table_uuid)),
        location(std::move(location)),
        last_sequence_number(last_sequence_number),
        last_updated_ms(last_updated_ms),
        last_column_id(last_column_id),
        schemas(std::move(schemas)),
        current_schema_id(current_schema_id),
        default_spec_id(default_spec_id),
        last_partition_id(last_partition_id),
        properties(std::move(properties)),
        current_snapshot_id(current_snapshot_id),
        snapshots(std::move(snapshots)),
        snapshot_log(std::move(snapshot_log)),
        metadata_log(std::move(metadata_log)),
        default_sort_order_id(default_sort_order_id) {}

  std::optional<std::string> GetCurrentManifestListPath() const;

  std::shared_ptr<Schema> GetCurrentSchema() const;

  std::string table_uuid;
  std::string location;
  int64_t last_sequence_number;
  int64_t last_updated_ms;
  int32_t last_column_id;
  std::vector<std::shared_ptr<Schema>> schemas;
  int32_t current_schema_id;
  // std::vector<PartitionSpec> partition_specs_;
  int32_t default_spec_id;
  int32_t last_partition_id;
  std::map<std::string, std::string> properties;
  std::optional<int64_t> current_snapshot_id;
  // std::optional<std::vector<std::string>> valid_snapshot_paths_;
  std::vector<std::shared_ptr<Snapshot>> snapshots;
  std::optional<std::vector<std::pair<int64_t, int64_t>>> snapshot_log;
  std::optional<std::vector<std::pair<int64_t, std::string>>> metadata_log;
  // std::vector<SortOrder> sort_orders_;
  int32_t default_sort_order_id;
  // std::optional<std::map<std::string, SnapshotReference>> refs_;
  // std::optional<std::vector<Statistics>> statistics_;
};

struct TableMetadataV2Builder {
  TableMetadataV2Builder() = default;

  TableMetadataV2Builder(const TableMetadataV2Builder&) = delete;
  TableMetadataV2Builder& operator=(const TableMetadataV2Builder&) = delete;

  std::shared_ptr<TableMetadataV2> Build();

  std::optional<std::string> table_uuid;
  std::optional<std::string> location;
  std::optional<int64_t> last_sequence_number;
  std::optional<int64_t> last_updated_ms;
  std::optional<int32_t> last_column_id;
  std::optional<std::vector<std::shared_ptr<Schema>>> schemas;
  std::optional<int32_t> current_schema_id;
  /* partition spec builder */
  std::optional<int32_t> default_spec_id;
  std::optional<int32_t> last_partition_id;
  std::optional<std::map<std::string, std::string>> properties;
  std::optional<int64_t> current_snapshot_id;
  std::optional<std::vector<std::shared_ptr<Snapshot>>> snapshots;
  std::optional<std::vector<std::pair<int64_t, int64_t>>> snapshot_log;
  std::optional<std::vector<std::pair<int64_t, std::string>>> metadata_log;
  std::optional<int32_t> default_sort_order_id;
};

std::shared_ptr<TableMetadataV2> MakeTableMetadataV2(const std::string& json);

}  // namespace iceberg
