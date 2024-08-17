#include "iceberg/manifest_entry.h"
#include "iceberg/schema.h"

#include <fstream>
#include <sstream>

#include "iceberg/generated/manifest_entry.hh"
#include "iceberg/generated/manifest_entry_schema.h"

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/ValidSchema.hh"

namespace iceberg::ice_tea {

namespace {

ManifestEntry Convert(iceberg::manifest_entry&& manifest_entry) {
  ManifestEntry entry;
  entry.status = static_cast<ManifestEntry::Status>(manifest_entry.status);
  if (!manifest_entry.snapshot_id.is_null()) {
    entry.snapshot_id = manifest_entry.snapshot_id.get_long();
  }
  if (!manifest_entry.sequence_number.is_null()) {
    entry.sequence_number = manifest_entry.sequence_number.get_long();
  }
  if (!manifest_entry.file_sequence_number.is_null()) {
    entry.file_sequence_number = manifest_entry.file_sequence_number.get_long();
  }

  DataFile data_file;
  const auto& manifest_data_file = manifest_entry.data_file;
  data_file.content = static_cast<DataFile::FileContent>(manifest_data_file.content);
  data_file.file_path = manifest_data_file.file_path;
  data_file.file_format = manifest_data_file.file_format;
  data_file.record_count = manifest_data_file.record_count;
  data_file.file_size_in_bytes = manifest_data_file.file_size_in_bytes;
  if (!manifest_data_file.column_sizes.is_null()) {
    auto kv_array = manifest_data_file.column_sizes.get_array();
    for (auto&& kv : kv_array) {
      data_file.column_sizes.emplace(std::move(kv.key), std::move(kv.value));
    }
  }
  if (!manifest_data_file.value_counts.is_null()) {
    auto kv_array = manifest_data_file.value_counts.get_array();
    for (auto&& kv : kv_array) {
      data_file.value_counts.emplace(std::move(kv.key), std::move(kv.value));
    }
  }
  if (!manifest_data_file.split_offsets.is_null()) {
    auto offsets_array = manifest_data_file.split_offsets.get_array();
    for (auto&& offset : offsets_array) {
      data_file.split_offsets.emplace_back(offset);
    }
  }
  if (!manifest_data_file.equality_ids.is_null()) {
    auto ids_array = manifest_data_file.equality_ids.get_array();
    for (auto&& id : ids_array) {
      data_file.equality_ids.emplace_back(id);
    }
  }
  if (!manifest_data_file.lower_bounds.is_null()) {
    auto kv_array = manifest_data_file.lower_bounds.get_array();
    for (auto&& kv : kv_array) {
      data_file.lower_bounds.emplace(std::move(kv.key), std::move(kv.value));
    }
  }
  if (!manifest_data_file.upper_bounds.is_null()) {
    auto kv_array = manifest_data_file.upper_bounds.get_array();
    for (auto&& kv : kv_array) {
      data_file.upper_bounds.emplace(std::move(kv.key), std::move(kv.value));
    }
  }
  if (!manifest_data_file.null_value_counts.is_null()) {
    auto kv_array = manifest_data_file.null_value_counts.get_array();
    for (auto&& kv : kv_array) {
      data_file.null_value_counts.emplace(std::move(kv.key), std::move(kv.value));
    }
  }
  if (!manifest_data_file.nan_value_counts.is_null()) {
    auto kv_array = manifest_data_file.nan_value_counts.get_array();
    for (auto&& kv : kv_array) {
      data_file.nan_value_counts.emplace(std::move(kv.key), std::move(kv.value));
    }
  }
  if (!manifest_data_file.distinct_counts.is_null()) {
    auto kv_array = manifest_data_file.distinct_counts.get_array();
    for (auto&& kv : kv_array) {
      data_file.distinct_counts.emplace(std::move(kv.key), std::move(kv.value));
    }
  }
  if (!manifest_data_file.key_metadata.is_null()) {
    data_file.key_metadata = manifest_data_file.key_metadata.get_bytes();
  }
  if (!manifest_data_file.sort_order_id.is_null()) {
    data_file.sort_order_id = manifest_data_file.sort_order_id.get_int();
  }
  entry.data_file = std::move(data_file);
  return entry;
}

iceberg::manifest_entry Convert(const ManifestEntry& entry) {
  iceberg::manifest_entry manifest_entry;

  manifest_entry.status = static_cast<int32_t>(entry.status);
  if (entry.snapshot_id) {
    manifest_entry.snapshot_id.set_long(*entry.snapshot_id);
  }
  if (entry.sequence_number) {
    manifest_entry.sequence_number.set_long(*entry.sequence_number);
  }
  if (entry.file_sequence_number) {
    manifest_entry.file_sequence_number.set_long(*entry.file_sequence_number);
  }

  const DataFile& data_file = entry.data_file;
  auto& manifest_data_file = manifest_entry.data_file;
  manifest_data_file.content = static_cast<int32_t>(data_file.content);
  manifest_data_file.file_path = data_file.file_path;
  manifest_data_file.file_format = data_file.file_format;
  manifest_data_file.record_count = data_file.record_count;
  manifest_data_file.file_size_in_bytes = data_file.file_size_in_bytes;

  if (!data_file.column_sizes.empty()) {
    std::decay_t<decltype(manifest_data_file.column_sizes.get_array())> vec;
    vec.reserve(data_file.column_sizes.size());
    for (auto& [key, value] : data_file.column_sizes) {
      decltype(vec)::value_type kv;
      kv.key = key;
      kv.value = value;
      vec.emplace_back(std::move(kv));
    }
    manifest_data_file.column_sizes.set_array(vec);
  }
  if (!data_file.value_counts.empty()) {
    std::decay_t<decltype(manifest_data_file.value_counts.get_array())> vec;
    vec.reserve(data_file.value_counts.size());
    for (auto& [key, value] : data_file.value_counts) {
      decltype(vec)::value_type kv;
      kv.key = key;
      kv.value = value;
      vec.emplace_back(std::move(kv));
    }
    manifest_data_file.value_counts.set_array(vec);
  }
  if (!data_file.split_offsets.empty()) {
    manifest_data_file.split_offsets.set_array(data_file.split_offsets);
  }
  if (!data_file.equality_ids.empty()) {
    manifest_data_file.equality_ids.set_array(data_file.equality_ids);
  }
  if (!data_file.lower_bounds.empty()) {
    std::decay_t<decltype(manifest_data_file.lower_bounds.get_array())> vec;
    vec.reserve(data_file.lower_bounds.size());
    for (auto& [key, value] : data_file.lower_bounds) {
      decltype(vec)::value_type kv;
      kv.key = key;
      kv.value = value;
      vec.emplace_back(std::move(kv));
    }
    manifest_data_file.lower_bounds.set_array(vec);
  }
  if (!data_file.upper_bounds.empty()) {
    std::decay_t<decltype(manifest_data_file.upper_bounds.get_array())> vec;
    vec.reserve(data_file.upper_bounds.size());
    for (auto& [key, value] : data_file.upper_bounds) {
      decltype(vec)::value_type kv;
      kv.key = key;
      kv.value = value;
      vec.emplace_back(std::move(kv));
    }
    manifest_data_file.upper_bounds.set_array(vec);
  }
  if (!data_file.null_value_counts.empty()) {
    std::decay_t<decltype(manifest_data_file.null_value_counts.get_array())> vec;
    vec.reserve(data_file.null_value_counts.size());
    for (auto& [key, value] : data_file.null_value_counts) {
      decltype(vec)::value_type kv;
      kv.key = key;
      kv.value = value;
      vec.emplace_back(std::move(kv));
    }
    manifest_data_file.null_value_counts.set_array(vec);
  }
  if (!data_file.nan_value_counts.empty()) {
    std::decay_t<decltype(manifest_data_file.nan_value_counts.get_array())> vec;
    vec.reserve(data_file.nan_value_counts.size());
    for (auto& [key, value] : data_file.nan_value_counts) {
      decltype(vec)::value_type kv;
      kv.key = key;
      kv.value = value;
      vec.emplace_back(std::move(kv));
    }
    manifest_data_file.nan_value_counts.set_array(vec);
  }
  if (!data_file.distinct_counts.empty()) {
    std::decay_t<decltype(manifest_data_file.distinct_counts.get_array())> vec;
    vec.reserve(data_file.distinct_counts.size());
    for (auto& [key, value] : data_file.distinct_counts) {
      decltype(vec)::value_type kv;
      kv.key = key;
      kv.value = value;
      vec.emplace_back(std::move(kv));
    }
    manifest_data_file.distinct_counts.set_array(vec);
  }
  if (!data_file.key_metadata.empty()) {
    manifest_data_file.key_metadata.set_bytes(data_file.key_metadata);
  }
  if (data_file.sort_order_id) {
    manifest_data_file.sort_order_id.set_int(*data_file.sort_order_id);
  }
  return manifest_entry;
}

avro::ValidSchema ManifestEntrySchema() {
  avro::ValidSchema result;
  std::stringstream in(kMetadataEntrySchemaJson.data());
  avro::compileJsonSchema(in, result);
  return result;
}

}  // namespace

std::vector<ManifestEntry> ReadManifestEntries(std::istream& input) {
  auto istream = avro::istreamInputStream(input);
  avro::DataFileReader<iceberg::manifest_entry> data_file_reader(std::move(istream), ManifestEntrySchema());
  std::vector<ManifestEntry> result;
  iceberg::manifest_entry manifest_entry;
  while (data_file_reader.read(manifest_entry)) {
    ManifestEntry entry = Convert(std::move(manifest_entry));
    result.emplace_back(std::move(entry));
  }
  return result;
}

std::vector<ManifestEntry> ReadManifestEntries(const std::string& data) {
  std::stringstream ss(data);
  return ReadManifestEntries(ss);
}

std::string WriteManifestEntries(const std::vector<ManifestEntry>& manifest_entries) {
  static constexpr size_t bufferSize = 1024 * 1024;

  std::stringstream ss;
  auto ostream = avro::ostreamOutputStream(ss, bufferSize);
  avro::DataFileWriter<iceberg::manifest_entry> writer(std::move(ostream), ManifestEntrySchema());

  for (auto& man_entry : manifest_entries) {
    writer.write(Convert(man_entry));
  }
  writer.close();

  return ss.str();
}

}  // namespace iceberg::ice_tea
