#pragma once

#include <algorithm>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/result.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"

namespace iceberg::ice_tea {

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
  DataEntry(std::string other_path, std::vector<Segment> other_parts)
      : path(std::move(other_path)), parts(std::move(other_parts)) {}

  DataEntry& operator+=(const DataEntry& other);

  std::string path;
  std::vector<Segment> parts;

  bool operator==(const DataEntry& other) const = default;

  inline void SortParts() {
    std::sort(parts.begin(), parts.end(), [&](const auto& lhs, const auto& rhs) { return lhs.offset < rhs.offset; });
  }
};

DataEntry operator+(const DataEntry& lhs, const DataEntry& rhs);

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

arrow::Result<ScanMetadata> GetScanMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                            const std::string& metadata_location,
                                            const GetScanMetadataConfig& config = {});

class IcebergEntriesStream {
 public:
  virtual std::optional<ManifestEntry> ReadNext() = 0;

  virtual ~IcebergEntriesStream() = default;
};

class AllEntriesStream : public IcebergEntriesStream {
 public:
  AllEntriesStream(std::shared_ptr<arrow::fs::FileSystem> fs, std::queue<ManifestFile> manifest_files,
                   const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs = {},
                   std::shared_ptr<iceberg::Schema> schema = {}, const ManifestEntryDeserializerConfig& config = {})
      : fs_(fs),
        manifest_files_(std::move(manifest_files)),
        partition_specs_(partition_specs),
        schema_(schema),
        config_(config) {}

  static std::shared_ptr<AllEntriesStream> Make(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                const std::string& manifest_list_path,
                                                const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs = {},
                                                std::shared_ptr<iceberg::Schema> schema = nullptr,
                                                const ManifestEntryDeserializerConfig& config = {});

  static std::shared_ptr<AllEntriesStream> Make(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                std::shared_ptr<TableMetadataV2> table_metadata,
                                                const ManifestEntryDeserializerConfig& config = {});

  std::optional<ManifestEntry> ReadNext();

 private:
  std::shared_ptr<arrow::fs::FileSystem> fs_;

  std::queue<ManifestFile> manifest_files_;

  ManifestFile current_manifest_file;
  std::queue<ManifestEntry> entries_for_current_manifest_file_;

  std::shared_ptr<iceberg::Schema> schema_;
  const std::vector<std::shared_ptr<PartitionSpec>> partition_specs_;
  ManifestEntryDeserializerConfig config_;
};

arrow::Result<ScanMetadata> GetScanMetadata(IcebergEntriesStream& entries_stream,
                                            const TableMetadataV2& table_metadata);

}  // namespace iceberg::ice_tea
