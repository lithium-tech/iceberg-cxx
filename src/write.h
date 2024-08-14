#pragma once

#include <filesystem>
#include <memory>
#include <vector>

#include "src/manifest_entry.h"
#include "src/manifest_file.h"
#include "src/table_metadata.h"

namespace iceberg::ice_tea {

void WriteMetadataFile(const std::filesystem::path& out_path, const std::shared_ptr<TableMetadataV2>& table_metadata);
void WriteManifestList(const std::filesystem::path& out_path, const std::vector<ManifestFile>& manifests);
void WriteManifest(const std::filesystem::path& out_path, const std::vector<ManifestEntry>& entries);

}  // namespace iceberg::ice_tea
