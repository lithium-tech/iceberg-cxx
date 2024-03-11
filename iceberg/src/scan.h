#pragma once

#include <string>

#include "arrow/filesystem/s3fs.h"
#include "arrow/result.h"
#include "iceberg/src/hive_client.h"
#include "iceberg/src/manifest_entry.h"

namespace iceberg {

// TODO(gmusya): support arrow::fs::FileSystem
arrow::Result<std::vector<ManifestEntry>> GetAllEntries(
    const std::string& db_name, const std::string& table_name,
    std::shared_ptr<arrow::fs::S3FileSystem> s3fs,
    const HiveClient& hive_client);

}  // namespace iceberg
