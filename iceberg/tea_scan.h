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

    int64_t offset;
    int64_t length;  // 0 <=> until end
  };

  DataEntry() = delete;
  DataEntry(std::string p) : path(std::move(p)) {}
  DataEntry(std::string other_path, std::vector<Segment> other_parts)
      : path(std::move(other_path)), parts(std::move(other_parts)) {}

  std::string path;
  std::vector<Segment> parts;

  inline void SortParts() {
    std::sort(parts.begin(), parts.end(), [&](const auto& lhs, const auto& rhs) { return lhs.offset < rhs.offset; });
  }
};

struct PositionalDeleteInfo {
  std::string path;

  PositionalDeleteInfo(std::string p) : path(std::move(p)) {}
};

struct EqualityDeleteInfo {
  std::string path;
  std::vector<int32_t> field_ids;

  EqualityDeleteInfo(std::string p, std::vector<int32_t> f) : path(std::move(p)), field_ids(std::move(f)) {}
};

struct ScanMetadata {
  struct Layer {
    std::vector<DataEntry> data_entries_;
    std::vector<PositionalDeleteInfo> positional_delete_entries_;
    std::vector<EqualityDeleteInfo> equality_delete_entries_;
  };

  using Partition = std::vector<Layer>;

  std::shared_ptr<Schema> schema;
  std::vector<Partition> partitions;
};

arrow::Result<std::string> ReadFile(std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& url);

arrow::Result<ScanMetadata> GetScanMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                            const std::string& metadata_location);

class IcebergEntriesStream {
 public:
  virtual std::optional<ManifestEntry> ReadNext() = 0;

  virtual ~IcebergEntriesStream() = default;
};

class AllEntriesStream : public IcebergEntriesStream {
 public:
  AllEntriesStream(std::shared_ptr<arrow::fs::FileSystem> fs, std::queue<ManifestFile> manifest_files)
      : fs_(fs), manifest_files_(std::move(manifest_files)) {}

  static std::shared_ptr<AllEntriesStream> Make(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                const std::string& manifest_list_path);

  static std::shared_ptr<AllEntriesStream> Make(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                std::shared_ptr<TableMetadataV2> table_metadata);

  std::optional<ManifestEntry> ReadNext();

 private:
  std::shared_ptr<arrow::fs::FileSystem> fs_;

  std::queue<ManifestFile> manifest_files_;

  ManifestFile current_manifest_file;
  std::queue<ManifestEntry> entries_for_current_manifest_file_;
};

arrow::Result<ScanMetadata> GetScanMetadata(IcebergEntriesStream& entries_stream,
                                            const TableMetadataV2& table_metadata);

}  // namespace iceberg::ice_tea
