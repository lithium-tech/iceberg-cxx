#pragma once

#include <optional>
#include <string>
#include <utility>

namespace iceberg {

class TableMetadataV2 {
  friend class TableMetadataV2Builder;

 public:
  const std::string& GetTableUUID() const { return table_uuid_; }
  const std::string& GetLocation() const { return location_; }

 private:
  TableMetadataV2(std::string table_uuid, std::string location)
      : table_uuid_(std::move(table_uuid)), location_(std::move(location)) {}

  std::string table_uuid_;
  std::string location_;
  // int64_t last_sequence_number_;
  // int64_t last_updated_ms_;
  // int32_t last_columnd_id_;
  // std::vector<Schema> schemas_;
  // int32_t current_schema_id;
  // std::vector<PartitionSpec> partition_specs_;
  // int32_t default_spec_id_;
  // int32_t last_partition_id_;
  // std::optional<std::map<std::string, std::string>> properties_;
  // std::optional<int64_t> current_snapshot_id_;
  // std::optional<std::vector<std::string>> valid_snapshot_paths_;
  // std::optional<std::vector<std::pair<int64_t, int64_t>> snapshot_log_;
  // std::optional<std::vector<std::pair<int64_t, std::string>> metadata_log_;
  // std::vector<SortOrder> sort_orders_;
  // int32_t default_sort_order_id_;
  // std::optional<std::map<std::string, SnapshotReference>> refs_;
  // std::optional<std::vector<Statistics>> statistics_;
};

class TableMetadataV2Builder {
 public:
  TableMetadataV2Builder() = default;

  TableMetadataV2Builder(const TableMetadataV2Builder&) = delete;
  TableMetadataV2Builder& operator=(const TableMetadataV2Builder&) = delete;

  TableMetadataV2 Build() &&;

  void SetTableUUID(std::string table_uuid) {
    table_uuid_ = std::move(table_uuid);
  }
  void SetLocation(std::string location) { location_ = std::move(location); }

 private:
  std::optional<std::string> table_uuid_;
  std::optional<std::string> location_;
};

TableMetadataV2 MakeMetadata(const std::string& json);

}  // namespace iceberg
