#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/puffin.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::vector<T>& p);

template <>
std::ostream& operator<<(std::ostream& os, const std::vector<uint8_t>& p);

template <typename K, typename V>
std::ostream& operator<<(std::ostream& os, const std::map<K, V>& p);

std::ostream& operator<<(std::ostream& os, const iceberg::types::NestedField& field) {
  os << "Field(" << field.name << ", " << field.field_id << ", " << (field.is_required ? "required" : "nullable")
     << ", " << field.type->ToString() << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, std::shared_ptr<iceberg::Schema> schema) {
  os << "Schema(id = " << schema->SchemaId() << ", columns = ";
  os << schema->Columns() << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, std::shared_ptr<iceberg::Snapshot> snapshot) {
  os << "Snapshot(" << snapshot->snapshot_id << ", " << snapshot->parent_snapshot_id.value_or(-1) << ", "
     << snapshot->sequence_number << ", " << snapshot->timestamp_ms << ", " << snapshot->manifest_list_location << ", "
     << snapshot->summary << ", " << snapshot->schema_id.value_or(-1) << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const iceberg::PartitionFieldSummary& summary) {
  os << "PartitionFieldSummary(";
  os << summary.contains_null << ", ";
  os << summary.contains_nan.value_or(false) << ", ";
  os << summary.lower_bound << ", ";
  os << summary.upper_bound;
  os << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const iceberg::ManifestFile& manifest) {
  os << "ManifestFile(";
  os << "added_files_count: " << manifest.added_files_count << ", ";
  os << "added_rows_count: " << manifest.added_rows_count << ", ";
  os << "content: " << static_cast<int>(manifest.content) << ", ";
  os << "deleted_files_count: " << manifest.deleted_files_count << ", ";
  os << "deleted_rows_count: " << manifest.deleted_rows_count << ", ";
  os << "existing_files_count: " << manifest.existing_files_count << ", ";
  os << "existing_rows_count: " << manifest.existing_rows_count << ", ";
  os << "length: " << manifest.length << ", ";
  os << "min_sequence_number: " << manifest.min_sequence_number << ", ";
  os << "partition_spec_id: " << manifest.partition_spec_id << ", ";
  os << "path: " << manifest.path << ", ";
  os << "sequence_number: " << manifest.sequence_number << ", ";
  os << "snapshot_id: " << manifest.snapshot_id << ", ";
  os << "partitions: " << manifest.partitions;
  os << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const iceberg::ContentFile& entry) {
  os << "ContentFile(";
  os << "content: " << static_cast<int>(entry.content) << ", ";
  os << "file_path: " << entry.file_path << ", ";
  os << "file_format: " << entry.file_format << ", ";
  os << "record_count: " << entry.record_count << ", ";
  os << "file_size_in_bytes: " << entry.file_size_in_bytes << ", ";
  os << "column_sizes: " << entry.column_sizes << ", ";
  os << "value_counts: " << entry.value_counts << ", ";
  os << "null_value_counts: " << entry.null_value_counts << ", ";
  os << "nan_value_counts: " << entry.nan_value_counts << ", ";
  os << "distinct_counts: " << entry.distinct_counts << ", ";
  os << "lower_bounds: " << entry.lower_bounds << ", ";
  os << "upper_bounds: " << entry.upper_bounds << ", ";
  os << "key_metadata: " << entry.key_metadata << ", ";
  os << "split_offsets: " << entry.split_offsets << ", ";
  os << "equality_ids: " << entry.equality_ids << ", ";
  os << "sort_order_id: " << entry.equality_ids;
  os << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const iceberg::ManifestEntry& entry) {
  os << "ManifestEntry(";
  os << "status: " << static_cast<int>(entry.status) << ", ";
  os << "snapshot_id: " << entry.snapshot_id.value_or(-1) << ", ";
  os << "sequence_number: " << entry.sequence_number.value_or(-1) << ", ";
  os << "file_sequence_number: " << entry.file_sequence_number.value_or(-1) << ", ";
  os << "data_file: " << entry.data_file;
  os << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const iceberg::PuffinFile::Footer::BlobMetadata& blob_metadata) {
  os << "PuffinBlobMetadata(";
  os << "type: " << blob_metadata.type << ", ";
  os << "fields: " << blob_metadata.fields << ", ";
  os << "snapshot_id: " << blob_metadata.snapshot_id << ", ";
  os << "sequence_number: " << blob_metadata.sequence_number << ", ";
  os << "offset: " << blob_metadata.offset << ", ";
  os << "length: " << blob_metadata.length << ", ";
  os << "compression_codec: " << blob_metadata.compression_codec.value_or("uncompressed") << ", ";
  os << "properties: " << blob_metadata.properties;
  os << ")";
  return os;
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::vector<T>& p) {
  os << "[";
  bool is_first = true;
  for (const auto& elem : p) {
    if (is_first) {
      is_first = false;
    } else {
      os << ", ";
    }
    os << elem;
  }
  os << "]";
  return os;
}

template <>
std::ostream& operator<<(std::ostream& os, const std::vector<uint8_t>& p) {
  os << "bytes(" << p.size() << ")";
  return os;
}

template <typename K, typename V>
std::ostream& operator<<(std::ostream& os, const std::map<K, V>& p) {
  os << "{";
  bool is_first = true;
  for (const auto& [k, v] : p) {
    if (is_first) {
      is_first = false;
    } else {
      os << ", ";
    }
    os << k << ": " << v;
  }
  os << "}";
  return os;
}

ABSL_FLAG(std::string, mode, "", "mode");

ABSL_FLAG(std::string, snapshot_location, "", "snapshot file location");

ABSL_FLAG(std::string, manifest_list_location, "", "manifest list file location");

ABSL_FLAG(std::string, manifest_entry_location, "", "manifest entry file location");

ABSL_FLAG(std::string, stats_file_location, "", "stats file location");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  const std::string mode = absl::GetFlag(FLAGS_mode);
  if (mode.empty()) {
    std::cerr << "mode is not set" << std::endl;
    return 1;
  }

  if (mode == "print-snapshot") {
    const std::string snapshot_location = absl::GetFlag(FLAGS_snapshot_location);

    if (snapshot_location.empty()) {
      std::cerr << "snapshot_location is not set" << std::endl;
      return 1;
    }

    std::ifstream ss(snapshot_location, std::ios::binary);

    auto table_metadata = iceberg::ice_tea::ReadTableMetadataV2(ss);
    if (!table_metadata) {
      std::cerr << "Failed to open file" << std::endl;
      return 1;
    }

    std::cout << "format_vesion: " << table_metadata->format_vesion << std::endl;
    std::cout << "table_uuid: " << table_metadata->table_uuid << std::endl;
    std::cout << "last_sequence_number: " << table_metadata->last_sequence_number << std::endl;
    std::cout << "last_updated_ms: " << table_metadata->last_updated_ms << std::endl;
    std::cout << "last_column_id: " << table_metadata->last_column_id << std::endl;
    std::cout << "schemas: " << table_metadata->schemas << std::endl;
    std::cout << "current_schema_id: " << table_metadata->current_schema_id << std::endl;
    // std::cout << "partition_specs: " << table_metadata->partition_specs << std::endl;
    std::cout << "default_spec_id: " << table_metadata->default_spec_id << std::endl;
    std::cout << "last_partition_id: " << table_metadata->last_partition_id << std::endl;
    std::cout << "properties: " << table_metadata->properties << std::endl;
    std::cout << "snapshots: " << table_metadata->snapshots << std::endl;
    // std::cout << "snapshot_log: " << table_metadata->snapshot_log << std::endl;
    // std::cout << "metadata_log: " << table_metadata->metadata_log << std::endl;
    // std::cout << "sort_orders: " << table_metadata->sort_orders << std::endl;
    std::cout << "default_sort_order_id: " << table_metadata->default_sort_order_id << std::endl;
    // std::cout << "statistics: " << table_metadata->statistics << std::endl;
    // std::cout << "refs: " << table_metadata->refs << std::endl;
    return 0;
  }

  if (mode == "print-manifest-list") {
    const std::string manifest_list_location = absl::GetFlag(FLAGS_manifest_list_location);

    if (manifest_list_location.empty()) {
      std::cerr << "manifest_list_location is not set" << std::endl;
      return 1;
    }

    std::ifstream ss(manifest_list_location, std::ios::binary);

    auto list = iceberg::ice_tea::ReadManifestList(ss);
    std::cout << list << std::endl;
    return 0;
  }

  if (mode == "print-manifest-entry") {
    const std::string manifest_entry_location = absl::GetFlag(FLAGS_manifest_entry_location);

    if (manifest_entry_location.empty()) {
      std::cerr << "manifest_entry_location is not set" << std::endl;
      return 1;
    }

    std::ifstream ss(manifest_entry_location, std::ios::binary);

    auto list = iceberg::ice_tea::ReadManifestEntries(ss);
    std::cout << "metadata: " << list.metadata << std::endl;
    std::cout << "entries: " << list.entries << std::endl;
    return 0;
  }

  if (mode == "print-stats-file") {
    const std::string stats_file_location = absl::GetFlag(FLAGS_stats_file_location);

    if (stats_file_location.empty()) {
      std::cerr << "stats_file_location is not set" << std::endl;
      return 1;
    }

    std::ifstream is(stats_file_location, std::ios::binary);
    std::stringstream ss;
    ss << is.rdbuf();
    std::string data = ss.str();

    auto maybe_puffin_file = iceberg::PuffinFile::Make(data);
    if (!maybe_puffin_file.ok()) {
      std::cerr << maybe_puffin_file.status() << std::endl;
      return 1;
    }

    iceberg::PuffinFile puffin_file = maybe_puffin_file.MoveValueUnsafe();

    iceberg::PuffinFile::Footer footer = puffin_file.GetFooter();

    auto deserialized_footer = footer.GetDeserializedFooter();
    std::cout << "blobs: " << deserialized_footer.blobs << std::endl;
    std::cout << "properties: " << deserialized_footer.properties << std::endl;
    return 0;
  }

  std::cerr << "Unexpected mode" << std::endl;
  return 1;
}
