#include "iceberg/write.h"

#include <fstream>
#include <stdexcept>
#include <string>

#include "iceberg/common/error.h"

namespace iceberg::ice_tea {

namespace {

void WriteSerialized(std::shared_ptr<arrow::fs::FileSystem> fs, const std::filesystem::path& path,
                     const std::string& serialized_data) {
  auto output = fs->OpenOutputStream(path);
  Ensure(output.ok(), "Can not open file " + std::string(path));

  auto write_status = (*output)->Write(serialized_data.data(), serialized_data.size());
  Ensure(write_status.ok(), "Can not write to file " + std::string(path));

  auto close_status = (*output)->Close();
  Ensure(close_status.ok(), "Can not close file " + std::string(path));
}

}  // namespace

void WriteMetadataFile(const std::filesystem::path& out_path, const std::shared_ptr<TableMetadataV2>& table_metadata) {
  std::string serialized = iceberg::ice_tea::WriteTableMetadataV2(*table_metadata, true);
  std::ofstream ofstream(out_path);
  Ensure(!!ofstream, "Failed to write MetadataFile to " + out_path.string());

  ofstream.write(serialized.data(), serialized.size());
}

void WriteManifestList(const std::filesystem::path& out_path, const std::vector<ManifestFile>& manifests) {
  std::string serialized = iceberg::ice_tea::WriteManifestList(manifests);
  std::ofstream ofstream(out_path);
  Ensure(!!ofstream, "Failed to write WriteManifestList to " + out_path.string());

  ofstream.write(serialized.data(), serialized.size());
}

void WriteManifest(const std::filesystem::path& out_path, const Manifest& entries) {
  std::string serialized = iceberg::ice_tea::WriteManifestEntries(entries);
  std::ofstream ofstream(out_path);
  Ensure(!!ofstream, "Failed to write WriteManifest to " + out_path.string());

  ofstream.write(serialized.data(), serialized.size());
}

void WriteMetadataFileRemote(std::shared_ptr<arrow::fs::FileSystem> fs, const std::filesystem::path& out_path,
                             const std::shared_ptr<TableMetadataV2>& table_metadata) {
  std::string serialized = iceberg::ice_tea::WriteTableMetadataV2(*table_metadata, true);
  WriteSerialized(fs, out_path, serialized);
}

void WriteManifestListRemote(std::shared_ptr<arrow::fs::FileSystem> fs, const std::filesystem::path& out_path,
                             const std::vector<ManifestFile>& manifests) {
  std::string serialized = iceberg::ice_tea::WriteManifestList(manifests);
  WriteSerialized(fs, out_path, serialized);
}

void WriteManifestRemote(std::shared_ptr<arrow::fs::FileSystem> fs, const std::filesystem::path& out_path,
                         const Manifest& entries) {
  std::string serialized = iceberg::ice_tea::WriteManifestEntries(entries);
  WriteSerialized(fs, out_path, serialized);
}

}  // namespace iceberg::ice_tea
