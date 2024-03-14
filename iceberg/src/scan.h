#pragma once

#include <string>

#include "arrow/filesystem/s3fs.h"
#include "arrow/result.h"
#include "iceberg/src/hive_client.h"
#include "iceberg/src/manifest_entry.h"
#include "iceberg/src/schema.h"

namespace iceberg {

struct ScanMetadata {
  Schema schema;
  std::vector<ManifestEntry> entries;
};

// TODO(gmusya): support arrow::fs::FileSystem
arrow::Result<ScanMetadata> GetScanMetadata(
    const std::string& db_name, const std::string& table_name,
    std::shared_ptr<arrow::fs::S3FileSystem> s3fs,
    const HiveClient& hive_client);

}  // namespace iceberg
