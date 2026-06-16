#pragma once

#include <algorithm>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/result.h"
#include "iceberg/common/logger.h"
#include "iceberg/filter/stats_filter/stats_filter.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"

namespace iceberg::ice_tea {

using SequenceNumber = int64_t;

struct PositionalDeleteInfo {
  std::string path;

  PositionalDeleteInfo(std::string p) : path(std::move(p)) {}

  bool operator==(const PositionalDeleteInfo& other) const = default;
};

struct EqualityDeleteInfo {
  std::string path;
  std::vector<int32_t> field_ids;

  EqualityDeleteInfo(std::string p, std::vector<int32_t> f) : path(std::move(p)), field_ids(std::move(f)) {}

  bool operator==(const EqualityDeleteInfo& other) const = default;
};

struct DeletionVectorInfo {
  std::string path;
  int64_t offset;
  int64_t length;
  std::string referenced_data_file;

  bool operator==(const DeletionVectorInfo&) const = default;
};

struct DataEntry {
  struct Segment {
    Segment() = delete;
    Segment(int64_t off, int64_t len) : offset(off), length(len) {}

    bool operator==(const Segment& other) const = default;

    int64_t offset;
    int64_t length;  // 0 <=> until end
  };

  DataEntry(const DataEntry&) = default;
  DataEntry& operator=(const DataEntry&) = default;
  DataEntry(DataEntry&& other) = default;
  DataEntry& operator=(DataEntry&& other) = default;
  DataEntry() = delete;
  DataEntry(std::string p) : path(std::move(p)) {}
  DataEntry(std::string other_path, std::vector<Segment> other_parts,
            std::optional<DeletionVectorInfo> other_dv = std::nullopt)
      : path(std::move(other_path)), parts(std::move(other_parts)), dv(std::move(other_dv)) {}

  DataEntry& operator+=(const DataEntry& other);

  std::string path;
  std::vector<Segment> parts;
  std::optional<DeletionVectorInfo> dv;

  bool operator==(const DataEntry& other) const = default;

  inline void SortParts() {
    std::sort(parts.begin(), parts.end(), [&](const auto& lhs, const auto& rhs) { return lhs.offset < rhs.offset; });
  }
};

DataEntry operator+(const DataEntry& lhs, const DataEntry& rhs);

struct ScanMetadata {
  struct Layer {
    std::vector<DataEntry> data_entries_;
    std::vector<PositionalDeleteInfo> positional_delete_entries_;
    std::vector<EqualityDeleteInfo> equality_delete_entries_;

    bool operator==(const Layer& layer) const = default;

    bool Empty() const;
  };

  using Partition = std::vector<Layer>;

  std::shared_ptr<Schema> schema;
  std::vector<Partition> partitions;

  bool operator==(const ScanMetadata& scan_meta) const = default;
};

arrow::Result<std::string> ReadFile(std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& url);

struct GetScanMetadataConfig {
  ManifestEntryDeserializerConfig manifest_entry_deserializer_config;
};

std::vector<bool> FilterManifests(std::shared_ptr<filter::StatsFilter> stats_filter,
                                  std::shared_ptr<iceberg::Schema> schema,
                                  const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs,
                                  const std::vector<ManifestFile>& manifest_metadatas);

arrow::Result<ScanMetadata> GetScanMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                            const std::string& metadata_location,
                                            std::function<bool(iceberg::Schema& schema)> use_avro_reader_schema,
                                            std::shared_ptr<filter::StatsFilter> stats_filter = nullptr,
                                            const GetScanMetadataConfig& config = {},
                                            std::shared_ptr<ILogger> logger = nullptr);

class AllEntriesStream : public IcebergEntriesStream {
 public:
  AllEntriesStream(std::shared_ptr<arrow::fs::FileSystem> fs, std::queue<ManifestFile> manifest_files,
                   bool use_reader_schema, const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs = {},
                   std::shared_ptr<iceberg::Schema> schema = {}, const ManifestEntryDeserializerConfig& config = {})
      : fs_(fs),
        manifest_files_(std::move(manifest_files)),
        schema_(schema),
        partition_specs_(partition_specs),
        config_(config),
        use_avro_reader_schema_(use_reader_schema) {}

  static std::shared_ptr<AllEntriesStream> Make(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                const std::string& manifest_list_path, bool use_reader_schema,
                                                const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs = {},
                                                std::shared_ptr<iceberg::Schema> schema = nullptr,
                                                std::shared_ptr<filter::StatsFilter> stats_filter = nullptr,
                                                const ManifestEntryDeserializerConfig& config = {});

  static std::shared_ptr<AllEntriesStream> Make(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                std::shared_ptr<TableMetadataV2> table_metadata, bool use_reader_schema,
                                                std::shared_ptr<filter::StatsFilter> stats_filter = nullptr,
                                                const ManifestEntryDeserializerConfig& config = {});

  std::optional<ManifestEntry> ReadNext();

 private:
  std::shared_ptr<arrow::fs::FileSystem> fs_;

  std::queue<ManifestFile> manifest_files_;

  std::shared_ptr<IcebergEntriesStream> current_manifest_stream_;

  std::shared_ptr<iceberg::Schema> schema_;
  const std::vector<std::shared_ptr<PartitionSpec>> partition_specs_;
  ManifestEntryDeserializerConfig config_;
  bool use_avro_reader_schema_ = false;
};

arrow::Result<ScanMetadata> GetScanMetadata(IcebergEntriesStream& entries_stream, const TableMetadataV2& table_metadata,
                                            std::shared_ptr<ILogger> logger);

arrow::Result<ScanMetadata> GetScanMetadata(IcebergEntriesStream& entries_stream, const TableMetadataV2& table_metadata,
                                            std::shared_ptr<iceberg::Schema> schema, std::shared_ptr<ILogger> logger);

struct PositionalDeleteWithExtraInfo {
  PositionalDeleteInfo positional_delete_;
  std::optional<std::pair<std::string, std::string>> min_max_referenced_path_;

  PositionalDeleteWithExtraInfo(std::string path,
                                std::optional<std::pair<std::string, std::string>> min_max_referenced_path)
      : positional_delete_(std::move(path)), min_max_referenced_path_(std::move(min_max_referenced_path)) {}
};

struct LayerWithExtraInfo {
  std::vector<DataEntry> data_entries_;
  std::vector<PositionalDeleteWithExtraInfo> positional_delete_entries_;
  std::vector<EqualityDeleteInfo> equality_delete_entries_;
  std::unordered_map<std::string, DeletionVectorInfo> dv_entries_;

  bool operator==(const LayerWithExtraInfo& layer) const = default;

  bool Empty() const {
    return data_entries_.empty() && positional_delete_entries_.empty() && equality_delete_entries_.empty() &&
           dv_entries_.empty();
  }
};

class ScanMetadataBuilder {
 public:
  explicit ScanMetadataBuilder(const TableMetadataV2& table_metadata, std::shared_ptr<ILogger> logger)
      : ScanMetadataBuilder(table_metadata, table_metadata.GetCurrentSchema(), std::move(logger)) {}

  ScanMetadataBuilder(const TableMetadataV2& table_metadata, std::shared_ptr<iceberg::Schema> schema,
                      std::shared_ptr<ILogger> logger)
      : table_metadata_(table_metadata), schema_(std::move(schema)), logger_(std::move(logger)) {}

  virtual ~ScanMetadataBuilder() = default;

  ScanMetadata GetResult();

  arrow::Status AddEntry(const iceberg::ManifestEntry& entry);

  virtual void AddDataFile(const std::string& serialized_partition_key, SequenceNumber sequence_number,
                           const std::string& path, std::vector<DataEntry::Segment>&& segments);

  virtual void AddPositionDeletes(const std::string& serialized_partition_key, SequenceNumber sequence_number,
                                  const std::string& path,
                                  const std::optional<std::pair<std::string, std::string>>& min_max_referenced_path);

  virtual void AddGlobalEqualityDeletes(SequenceNumber sequence_number, const std::string& path,
                                        const std::vector<int>& equality_ids);

  virtual void AddEqualityDeletes(const std::string& serialized_partition_key, SequenceNumber sequence_number,
                                  const std::string& path, const std::vector<int>& equality_ids);

  virtual void AddDeletionVector(const std::string& serialized_partition_key, SequenceNumber sequence_number,
                                 const std::string& path, const std::string& referenced_data_file, int64_t offset,
                                 int64_t length);

 protected:
  arrow::Status CheckPartitionTupleIsCorrect(const iceberg::ManifestEntry& entry) const;

  static std::vector<std::vector<LayerWithExtraInfo>> MapToVec(
      std::map<std::string, std::map<SequenceNumber, LayerWithExtraInfo>>&& scan_metadata);

  std::vector<ScanMetadata::Partition> GetPartitions(
      std::map<std::string, std::map<SequenceNumber, LayerWithExtraInfo>>&& partitions,
      std::shared_ptr<ILogger> logger);

  std::vector<ScanMetadata::Partition> RemoveDanglingDeletes(std::vector<std::vector<LayerWithExtraInfo>>&& partitions,
                                                             std::shared_ptr<ILogger> logger);

  std::map<std::string, std::map<SequenceNumber, LayerWithExtraInfo>> partitions;
  // if there are k partitions and t global equality delete entries, k * t entries will be created
  // TODO(gmusya): improve
  std::map<SequenceNumber, std::vector<EqualityDeleteInfo>> global_equality_deletes;
  const TableMetadataV2& table_metadata_;
  std::shared_ptr<iceberg::Schema> schema_;
  std::shared_ptr<ILogger> logger_;

 private:
  void StoreEntry(std::string serialized_partition_key, const iceberg::ManifestEntry& entry);
  void ApplyGlobalEqualityDeletes(std::map<SequenceNumber, LayerWithExtraInfo>& layers) const;
  void ResolveDeletionVectors(std::map<SequenceNumber, LayerWithExtraInfo>& layers) const;
};

}  // namespace iceberg::ice_tea
