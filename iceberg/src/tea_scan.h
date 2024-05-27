#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/s3fs.h"
#include "arrow/result.h"
#include "iceberg/src/manifest_entry.h"
#include "iceberg/src/schema.h"
#include "iceberg/src/tea_hive_catalog.h"

namespace iceberg::ice_tea {

struct Task {
  struct Segment {
    Segment(int64_t off, int64_t len) : offset(off), length(len) {}

    int64_t offset;
    int64_t length;  // 0 <=> until end
  };

  ManifestEntry entry;
  std::vector<Segment> parts;  // empty <=> full file

  inline void SortParts() {
    std::sort(parts.begin(), parts.end(), [&](const auto& lhs, const auto& rhs) { return lhs.offset < rhs.offset; });
  }

  inline bool IsWholeFile() const { return parts.empty(); }
};

struct ScanMetadata {
  std::shared_ptr<Schema> schema;
  std::vector<Task> entries;
};

arrow::Result<std::string> ReadFile(std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& url);

arrow::Result<ScanMetadata> GetScanMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                            const std::string& metadata_location);

}  // namespace iceberg::ice_tea
