#include "iceberg/write.h"

#include <fstream>
#include <string>

namespace iceberg::ice_tea {

void WriteMetadataFile(const std::filesystem::path& out_path, const std::shared_ptr<TableMetadataV2>& table_metadata) {
  std::string serialized = iceberg::ice_tea::WriteTableMetadataV2(*table_metadata, true);
  std::ofstream ofstream(out_path);
  ofstream.write(serialized.data(), serialized.size());
}

void WriteManifestList(const std::filesystem::path& out_path, const std::vector<ManifestFile>& manifests) {
  std::string serialized = iceberg::ice_tea::WriteManifestList(manifests);
  std::ofstream ofstream(out_path);
  ofstream.write(serialized.data(), serialized.size());
}

void WriteManifest(const std::filesystem::path& out_path, const Manifest& entries) {
  std::string serialized = iceberg::ice_tea::WriteManifestEntries(entries);
  std::ofstream ofstream(out_path);
  ofstream.write(serialized.data(), serialized.size());
}

}  // namespace iceberg::ice_tea
