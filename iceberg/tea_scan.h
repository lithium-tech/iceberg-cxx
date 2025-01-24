#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/s3fs.h"
#include "arrow/result.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/tea_column_stats.h"
#include "iceberg/tea_hive_catalog.h"

namespace iceberg::ice_tea {

struct DataEntry {
  struct Segment {
    Segment() = delete;
    Segment(int64_t off, int64_t len) : offset(off), length(len) {}

    int64_t offset;
    int64_t length;  // 0 <=> until end
  };

  ColumnStats GetColumnStats(int32_t field_id) const;
  std::optional<int64_t> GetValueCounts(int32_t field_id) const;
  std::optional<int64_t> GetColumnSize(int32_t field_id) const;

  DataEntry() = delete;
  DataEntry(ManifestEntry e) : entry(std::move(e)) {}
  DataEntry(ManifestEntry e, std::vector<Segment> p) : entry(std::move(e)), parts(std::move(p)) {}

  ManifestEntry entry;
  std::vector<Segment> parts;

  inline void SortParts() {
    std::sort(parts.begin(), parts.end(), [&](const auto& lhs, const auto& rhs) { return lhs.offset < rhs.offset; });
  }
};

struct ScanMetadata {
  struct Layer {
    std::vector<DataEntry> data_entries_;
    std::vector<ManifestEntry> positional_delete_entries_;
    std::vector<ManifestEntry> equality_delete_entries_;
  };

  using Partition = std::vector<Layer>;

  std::shared_ptr<Schema> schema;
  std::vector<Partition> partitions;

  arrow::Result<ColumnStats> GetColumnStats(const std::string& column_name) const;
};

arrow::Result<std::string> ReadFile(std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& url);

arrow::Result<ScanMetadata> GetScanMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                            const std::string& metadata_location);

}  // namespace iceberg::ice_tea
