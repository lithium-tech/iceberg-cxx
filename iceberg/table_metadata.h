#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/transforms.h"

namespace iceberg {

struct SnapshotLog {
  int64_t timestamp_ms;
  int64_t snapshot_id;

  bool operator<(const SnapshotLog& other) const {
    return timestamp_ms < other.timestamp_ms || (timestamp_ms == other.timestamp_ms && snapshot_id < other.snapshot_id);
  }

  bool operator==(const SnapshotLog& other) const {
    return timestamp_ms == other.timestamp_ms && snapshot_id == other.snapshot_id;
  }
};

struct MetadataLog {
  int64_t timestamp_ms;
  std::string metadata_file;

  bool operator<(const MetadataLog& other) const {
    return timestamp_ms < other.timestamp_ms ||
           (timestamp_ms == other.timestamp_ms && metadata_file < other.metadata_file);
  }

  bool operator==(const MetadataLog& other) const {
    return timestamp_ms == other.timestamp_ms && metadata_file == other.metadata_file;
  }
};

enum class NullOrder { kNullsFirst, kNullsLast };
enum class SortDirection { kAsc, kDesc };

struct SortField {
  std::string transform;  // TODO(chertus): Transform type
  int32_t source_id{};
  SortDirection direction = SortDirection::kAsc;
  NullOrder null_order = NullOrder::kNullsLast;

  bool operator==(const SortField& other) const {
    return source_id == other.source_id && direction == other.direction && null_order == other.null_order &&
           transform == other.transform;
  }
};

struct SortOrder {
  int32_t order_id{};
  std::vector<SortField> fields;

  bool operator==(const SortOrder& other) const { return order_id == other.order_id && fields == other.fields; }

  std::vector<int32_t> FieldIds() const {
    std::vector<int32_t> ids;
    ids.reserve(fields.size());
    for (auto& field : fields) {
      ids.push_back(field.source_id);
    }
    return ids;
  }
};

struct PartitionField {
  int32_t source_id;
  int32_t field_id;
  std::string name;
  std::string transform;

  bool operator==(const PartitionField& other) const {
    return source_id == other.source_id && field_id == other.field_id && name == other.name &&
           transform == other.transform;
  }
};

struct PartitionSpec {
  int32_t spec_id;
  std::vector<PartitionField> fields;

  bool operator==(const PartitionSpec& other) const { return spec_id == other.spec_id && fields == other.fields; }
};

struct TableMetadataV2 {
  TableMetadataV2(std::string table_uuid_, std::string location_, int64_t last_sequence_number_,
                  int64_t last_updated_ms_, int32_t last_column_id_, std::vector<std::shared_ptr<Schema>>&& schemas_,
                  int32_t current_schema_id_, std::vector<std::shared_ptr<PartitionSpec>>&& partition_specs_,
                  int32_t default_spec_id_, int32_t last_partition_id_,
                  std::map<std::string, std::string>&& properties_, std::optional<int64_t> current_snapshot_id_,
                  std::vector<std::shared_ptr<Snapshot>>&& snapshots_, std::vector<SnapshotLog>&& snapshot_log_,
                  std::vector<MetadataLog>&& metadata_log_, std::vector<std::shared_ptr<SortOrder>>&& sort_orders_,
                  int32_t default_sort_order_id_, std::map<std::string, SnapshotRef>&& refs_)
      : table_uuid(std::move(table_uuid_)),
        location(std::move(location_)),
        last_sequence_number(last_sequence_number_),
        last_updated_ms(last_updated_ms_),
        last_column_id(last_column_id_),
        schemas(std::move(schemas_)),
        current_schema_id(current_schema_id_),
        partition_specs(std::move(partition_specs_)),
        default_spec_id(default_spec_id_),
        last_partition_id(last_partition_id_),
        properties(std::move(properties_)),
        current_snapshot_id(current_snapshot_id_),
        snapshots(std::move(snapshots_)),
        snapshot_log(std::move(snapshot_log_)),
        metadata_log(std::move(metadata_log_)),
        sort_orders(std::move(sort_orders_)),
        default_sort_order_id(default_sort_order_id_),
        refs(std::move(refs_)) {}

  std::optional<std::string> GetCurrentManifestListPath() const;
  std::shared_ptr<Schema> GetCurrentSchema() const;
  std::shared_ptr<SortOrder> GetSortOrder() const;
  int32_t SetSortOrder(std::shared_ptr<SortOrder> order);

  static constexpr uint32_t format_vesion = 2;                  // required
  std::string table_uuid;                                       // required
  std::string location;                                         // required
  int64_t last_sequence_number;                                 // required
  int64_t last_updated_ms;                                      // required
  int32_t last_column_id;                                       // required
  std::vector<std::shared_ptr<Schema>> schemas;                 // required
  int32_t current_schema_id;                                    // required
  std::vector<std::shared_ptr<PartitionSpec>> partition_specs;  // required
  int32_t default_spec_id;                                      // required
  int32_t last_partition_id;                                    // required
  std::map<std::string, std::string> properties;
  std::optional<int64_t> current_snapshot_id;
  std::vector<std::shared_ptr<Snapshot>> snapshots;
  std::vector<SnapshotLog> snapshot_log;
  std::vector<MetadataLog> metadata_log;
  std::vector<std::shared_ptr<SortOrder>> sort_orders;  // required
  int32_t default_sort_order_id;                        // required
  std::map<std::string, SnapshotRef> refs;
  // std::vector<Statistics> statistics_;
  // std::vector<PartitionStatistics> partition_statistics_;

  template <typename T>
  void FilterSchemaColumns(const T& filter) {
    for (auto& schema : schemas) {
      if (schema) {
        schema->FilterColumns(schema->FindColumnIds(filter));
      }
    }
  }
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
  std::optional<std::vector<std::shared_ptr<PartitionSpec>>> partition_specs;
  std::optional<int32_t> default_spec_id;
  std::optional<int32_t> last_partition_id;
  std::optional<std::map<std::string, std::string>> properties;
  std::optional<int64_t> current_snapshot_id;
  std::optional<std::vector<std::shared_ptr<Snapshot>>> snapshots;
  std::optional<std::vector<SnapshotLog>> snapshot_log;
  std::optional<std::vector<MetadataLog>> metadata_log;
  std::optional<std::vector<std::shared_ptr<SortOrder>>> sort_orders;
  std::optional<int32_t> default_sort_order_id;
  std::optional<std::map<std::string, SnapshotRef>> refs;
};

namespace ice_tea {

std::shared_ptr<TableMetadataV2> ReadTableMetadataV2(const std::string& json);
std::shared_ptr<TableMetadataV2> ReadTableMetadataV2(std::istream& istream);
std::string WriteTableMetadataV2(const TableMetadataV2& metadata, bool pretty = false);

}  // namespace ice_tea
}  // namespace iceberg
