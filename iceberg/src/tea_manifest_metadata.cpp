#include <sstream>
#include <utility>

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/ValidSchema.hh"
#include "iceberg/src/generated/manifest_file.hh"
#include "iceberg/src/generated/manifest_file_schema.h"
#include "iceberg/src/manifest_file.h"

namespace iceberg::ice_tea {
namespace {
ManifestFile Convert(const iceberg::manifest_file& manifest_file) {
  return ManifestFile{.added_files_count = (!manifest_file.added_files_count.is_null())
                                               ? manifest_file.added_files_count.get_int()
                                               : manifest_file.added_data_files_count.get_int(),
                      .added_rows_count = manifest_file.added_rows_count,
                      .content = (manifest_file.content == 0 ? ManifestContent::kData : ManifestContent::kDeletes),
                      .deleted_files_count = (!manifest_file.deleted_files_count.is_null())
                                                 ? manifest_file.deleted_files_count.get_int()
                                                 : manifest_file.deleted_data_files_count.get_int(),
                      .deleted_rows_count = manifest_file.deleted_rows_count,
                      .existing_files_count = (!manifest_file.existing_files_count.is_null())
                                                  ? manifest_file.existing_files_count.get_int()
                                                  : manifest_file.existing_data_files_count.get_int(),
                      .existing_rows_count = manifest_file.existing_rows_count,
                      .length = manifest_file.manifest_length,
                      .min_sequence_number = manifest_file.min_sequence_number,
                      .partition_spec_id = manifest_file.partition_spec_id,
                      .path = manifest_file.manifest_path,
                      .sequence_number = manifest_file.sequence_number,
                      .snapshot_id = manifest_file.added_snapshot_id};
}

iceberg::manifest_file Convert(const ManifestFile& manifest) {
  iceberg::manifest_file manifest_file;
  manifest_file.manifest_path = manifest.path;
  manifest_file.manifest_length = manifest.length;
  manifest_file.partition_spec_id = manifest.partition_spec_id;
  manifest_file.content = (manifest.content == ManifestContent::kData ? 0 : 1);
  manifest_file.sequence_number = manifest.sequence_number;
  manifest_file.min_sequence_number = manifest.min_sequence_number;
  manifest_file.added_snapshot_id = manifest.snapshot_id;
  manifest_file.added_files_count.set_int(manifest.added_files_count);
  manifest_file.existing_files_count.set_int(manifest.existing_files_count);
  manifest_file.deleted_files_count.set_int(manifest.deleted_files_count);
  manifest_file.added_rows_count = manifest.added_rows_count;
  manifest_file.existing_rows_count = manifest.existing_rows_count;
  manifest_file.deleted_rows_count = manifest.deleted_rows_count;
  return manifest_file;
}

avro::ValidSchema ManifestListSchema() {
  avro::ValidSchema result;
  std::stringstream in(kManifestListSchemaJson.data());
  avro::compileJsonSchema(in, result);
  return result;
};
}  // namespace

std::vector<ManifestFile> ReadManifestList(std::istream& input) {
  auto istream = avro::istreamInputStream(input);
  avro::DataFileReader<iceberg::manifest_file> data_file_reader(std::move(istream), ManifestListSchema());
  std::vector<ManifestFile> result;
  iceberg::manifest_file manifest_file;
  while (data_file_reader.read(manifest_file)) {
    ManifestFile manifest = Convert(manifest_file);
    result.emplace_back(std::move(manifest));
  }
  return result;
}

std::string WriteManifestList(const std::vector<ManifestFile>& manifest_list) {
  static constexpr size_t bufferSize = 1024 * 1024;

  std::stringstream ss;
  auto ostream = avro::ostreamOutputStream(ss, bufferSize);
  avro::DataFileWriter<iceberg::manifest_file> writer(std::move(ostream), ManifestListSchema());

  for (auto& manifest : manifest_list) {
    writer.write(Convert(manifest));
  }
  writer.close();

  return ss.str();
}

}  // namespace iceberg::ice_tea
