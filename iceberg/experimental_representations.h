#pragma once

#include <tuple>

#include "iceberg/schema.h"
#include "iceberg/tea_scan.h"
#include "iceberg/tea_scan_hashers.h"

namespace iceberg::experimental {

std::vector<ice_tea::DataEntry::Segment> UniteSegments(const std::vector<ice_tea::DataEntry::Segment>& segments);

bool AreDataEntriesEqual(const iceberg::ice_tea::DataEntry& lhs, const iceberg::ice_tea::DataEntry& rhs);

bool AreScanMetadataEqual(const iceberg::ice_tea::ScanMetadata& lhs, const iceberg::ice_tea::ScanMetadata& rhs);

}  // namespace iceberg::experimental
