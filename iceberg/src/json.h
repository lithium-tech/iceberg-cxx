#pragma once

#include <vector>

#include "iceberg/src/manifest_entry.h"
#include "iceberg/src/scan.h"

namespace iceberg {

std::string ScanMetadataToJSONString(const ScanMetadata& scan_metadata);

}  // namespace iceberg
