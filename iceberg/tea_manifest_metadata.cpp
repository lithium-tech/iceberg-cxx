#include <optional>
#include <sstream>
#include <utility>

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/ValidSchema.hh"
#include "iceberg/common/error.h"
#include "iceberg/generated/manifest_file.hh"
#include "iceberg/generated/manifest_file_schema.h"
#include "iceberg/manifest_file.h"

namespace iceberg::ice_tea {
namespace {

std::vector<PartitionFieldSummary> ConvertPartitions(const iceberg::manifest_file::partitions_t& partitions) {
  if (partitions.is_null()) {
    return {};
  }

  auto src = partitions.get_array();
  std::vector<PartitionFieldSummary> dst;
  dst.reserve(src.size());
  for (auto& p : src) {
    PartitionFieldSummary part{.contains_null = p.contains_null};
    if (!p.contains_nan.is_null()) {
      part.contains_nan = p.contains_nan.get_bool();
    }
    if (!p.lower_bound.is_null()) {
      part.lower_bound = p.lower_bound.get_bytes();
    }
    if (!p.upper_bound.is_null()) {
      part.upper_bound = p.upper_bound.get_bytes();
    }
    dst.emplace_back(std::move(part));
  }
  return dst;
}

void ConvertPartitions(const std::vector<PartitionFieldSummary>& partitions, iceberg::manifest_file& out) {
  if (partitions.empty()) {
    return;
  }

  // using TVec = decltype(out.partitions.get_array());
  using TVec = std::vector<iceberg::r508>;
  using TElem = typename TVec::value_type;

  TVec vec;
  for (auto& p : partitions) {
    TElem part;
    part.contains_null = p.contains_null;
    if (p.contains_nan) {
      part.contains_nan.set_bool(*p.contains_nan);
    }
    if (p.lower_bound.size()) {
      part.lower_bound.set_bytes(p.lower_bound);
    }
    if (p.upper_bound.size()) {
      part.upper_bound.set_bytes(p.upper_bound);
    }
    vec.emplace_back(std::move(part));
  }
  out.partitions.set_array(vec);
}

ManifestFile Convert(const iceberg::manifest_file& manifest_file) {
  return ManifestFile{.added_files_count = manifest_file.added_files_count ? manifest_file.added_files_count
                                                                           : manifest_file.added_data_files_count,
                      .added_rows_count = manifest_file.added_rows_count,
                      .content = (manifest_file.content == 0 ? ManifestContent::kData : ManifestContent::kDeletes),
                      .deleted_files_count = manifest_file.deleted_files_count ? manifest_file.deleted_files_count
                                                                               : manifest_file.deleted_data_files_count,
                      .deleted_rows_count = manifest_file.deleted_rows_count,
                      .existing_files_count = manifest_file.existing_files_count
                                                  ? manifest_file.existing_files_count
                                                  : manifest_file.existing_data_files_count,
                      .existing_rows_count = manifest_file.existing_rows_count,
                      .length = manifest_file.manifest_length,
                      .min_sequence_number = manifest_file.min_sequence_number,
                      .partition_spec_id = manifest_file.partition_spec_id,
                      .path = manifest_file.manifest_path,
                      .sequence_number = manifest_file.sequence_number,
                      .snapshot_id = manifest_file.added_snapshot_id,
                      .partitions = ConvertPartitions(manifest_file.partitions)};
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
  manifest_file.added_files_count = manifest.added_files_count;
  manifest_file.existing_files_count = manifest.existing_files_count;
  manifest_file.deleted_files_count = manifest.deleted_files_count;
  manifest_file.added_rows_count = manifest.added_rows_count;
  manifest_file.existing_rows_count = manifest.existing_rows_count;
  manifest_file.deleted_rows_count = manifest.deleted_rows_count;
#if 1  // Spark compatibility
  manifest_file.added_data_files_count = manifest.added_files_count;
  manifest_file.existing_data_files_count = manifest.existing_files_count;
  manifest_file.deleted_data_files_count = manifest.deleted_files_count;
#endif
  ConvertPartitions(manifest.partitions, manifest_file);
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
  Ensure(!!input, std::string(__FUNCTION__) + ": input is invalid");

  auto istream = avro::istreamInputStream(input);
  avro::DataFileReader<iceberg::manifest_file> data_file_reader(std::move(istream), ManifestListSchema());
  std::vector<ManifestFile> result;
  while (true) {
    iceberg::manifest_file manifest_file;
    if (data_file_reader.read(manifest_file)) {
      ManifestFile manifest = Convert(manifest_file);
      result.emplace_back(std::move(manifest));
    } else {
      break;
    }
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
