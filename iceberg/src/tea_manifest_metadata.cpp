#include <sstream>
#include <utility>

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/ValidSchema.hh"
#include "iceberg/src/generated/manifest_file.hh"
#include "iceberg/src/generated/manifest_file_schema.h"
#include "iceberg/src/manifest_file.h"

namespace iceberg::ice_tea {

std::vector<ManifestFile> MakeManifestList(const std::string& data) {
  avro::ValidSchema manifest_list_schema = []() {
    avro::ValidSchema result;
    std::stringstream in(kManifestListSchemaJson.data());
    avro::compileJsonSchema(in, result);
    return result;
  }();

  std::stringstream ss(data);
  auto istream = avro::istreamInputStream(ss);
  avro::DataFileReader<iceberg::manifest_file> data_file_reader(std::move(istream), manifest_list_schema);

  std::vector<ManifestFile> result;
  iceberg::manifest_file manifest_file;
  while (data_file_reader.read(manifest_file)) {
    ManifestFile manifest;
    manifest.path = manifest_file.manifest_path;
    manifest.length = manifest_file.manifest_length;
    manifest.partition_spec_id = manifest_file.partition_spec_id;
    manifest.content = manifest_file.content == 0 ? ManifestContent::kData : ManifestContent::kDeletes;
    manifest.sequence_number = manifest_file.sequence_number;
    manifest.min_sequence_number = manifest_file.min_sequence_number;
    manifest.snapshot_id = manifest_file.added_snapshot_id;
    manifest.added_files_count = manifest_file.added_files_count;
    manifest.existing_files_count = manifest_file.existing_files_count;
    manifest.deleted_files_count = manifest_file.deleted_files_count;
    manifest.added_rows_count = manifest_file.added_rows_count;
    manifest.existing_rows_count = manifest_file.existing_rows_count;
    manifest.deleted_rows_count = manifest_file.deleted_rows_count;
    result.emplace_back(std::move(manifest));
  }
  return result;
}

}  // namespace iceberg::ice_tea
