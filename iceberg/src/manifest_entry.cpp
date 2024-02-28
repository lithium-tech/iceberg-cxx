#include "iceberg/src/manifest_entry.h"

#include <fstream>
#include <sstream>

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/ValidSchema.hh"
#include "iceberg/src/manifest_entry_schema.h"
#include "iceberg/src/generated/manifest_entry.hh"

namespace iceberg {

namespace {
template <typename KV>
concept KeyValue = requires(KV a) {
  { a.key };
  { a.value };
};

template <typename K, typename V, KeyValue KV>
std::pair<K, V> KVToPair(KV kv) {
  return std::make_pair(std::move(kv.key), std::move(kv.value));
}
}  // namespace

std::vector<ManifestEntry> MakeManifestEntries(const std::string& data) {
  avro::ValidSchema manifest_entry_schema = []() {
    avro::ValidSchema result;
    std::stringstream in(kMetadataEntrySchemaJson.data());
    avro::compileJsonSchema(in, result);
    return result;
  }();

  std::stringstream ss(data);
  auto istream = avro::istreamInputStream(ss);
  avro::DataFileReader<iceberg::manifest_entry> data_file_reader(
      std::move(istream), manifest_entry_schema);
  std::vector<ManifestEntry> result;
  iceberg::manifest_entry manifest_entry;
  while (data_file_reader.read(manifest_entry)) {
    ManifestEntry entry;
    entry.status = static_cast<ManifestEntry::Status>(manifest_entry.status);
    if (!manifest_entry.snapshot_id.is_null()) {
      entry.snapshot_id = manifest_entry.snapshot_id.get_long();
    }
    if (!manifest_entry.sequence_number.is_null()) {
      entry.sequence_number = manifest_entry.sequence_number.get_long();
    }
    if (!manifest_entry.file_sequence_number.is_null()) {
      entry.file_sequence_number =
          manifest_entry.file_sequence_number.get_long();
    }
    DataFile data_file;
    const auto& manifest_data_file = manifest_entry.data_file;
    data_file.content =
        static_cast<DataFile::Content>(manifest_data_file.content);
    data_file.file_path = manifest_data_file.file_path;
    data_file.file_format = manifest_data_file.file_format;
    data_file.record_count = manifest_data_file.record_count;
    data_file.file_size_in_bytes = manifest_data_file.file_size_in_bytes;
    if (!manifest_data_file.column_sizes.is_null()) {
      data_file.column_sizes = std::vector<std::pair<int32_t, int64_t>>();
      const auto& kv_array = manifest_data_file.column_sizes.get_array();
      for (const auto& kv : kv_array) {
        data_file.column_sizes->emplace_back(KVToPair<int32_t, int64_t>(kv));
      }
    }
    if (!manifest_data_file.value_counts.is_null()) {
      data_file.value_counts = std::vector<std::pair<int32_t, int64_t>>();
      const auto& kv_array = manifest_data_file.value_counts.get_array();
      for (const auto& kv : kv_array) {
        data_file.value_counts->emplace_back(KVToPair<int32_t, int64_t>(kv));
      }
    }
    if (!manifest_data_file.split_offsets.is_null()) {
      data_file.split_offsets = std::vector<int64_t>();
      const auto& offsets_array = manifest_data_file.split_offsets.get_array();
      for (const auto& offset : offsets_array) {
        data_file.split_offsets->emplace_back(offset);
      }
    }
    if (!manifest_data_file.equality_ids.is_null()) {
      data_file.equality_ids = std::vector<int32_t>();
      const auto& ids_array = manifest_data_file.equality_ids.get_array();
      for (const auto& id : ids_array) {
        data_file.equality_ids->emplace_back(id);
      }
    }
    entry.data_file = std::move(data_file);
    result.emplace_back(std::move(entry));
  }
  return result;
}

}  // namespace iceberg
