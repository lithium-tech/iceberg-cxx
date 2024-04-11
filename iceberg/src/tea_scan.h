#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/s3fs.h"
#include "arrow/result.h"
#include "iceberg/src/manifest_entry.h"
#include "iceberg/src/schema.h"
#include "iceberg/src/tea_hive_catalog.h"

namespace iceberg::ice_tea {

struct ScanMetadata {
  Schema schema;
  std::vector<ManifestEntry> entries;
};

// TODO(gmusya): support arrow::fs::FileSystem
arrow::Result<ScanMetadata> GetScanMetadata(const std::string& metadata_location,
                                            std::shared_ptr<arrow::fs::S3FileSystem> s3fs);

}  // namespace iceberg::ice_tea
