#include "iceberg/tea_scan.h"

#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/filesystem/filesystem.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/tea_column_stats.h"
#include "iceberg/type.h"

namespace iceberg::ice_tea {

namespace {

struct UrlComponents {
  std::string schema;
  std::string location;
  std::string path;
};

UrlComponents SplitUrl(const std::string& url) {
  UrlComponents result;
  const auto schema_delimiter = std::string("://");
  auto delimiter_pos = url.find(schema_delimiter);
  std::string::size_type location_pos;
  if (delimiter_pos == std::string::npos) {
    location_pos = 0;
  } else {
    result.schema.assign(url, 0, delimiter_pos);
    location_pos = delimiter_pos + schema_delimiter.size();
  }
  auto non_location_pos = url.find_first_of("/?#", location_pos);
  result.location.assign(url, location_pos, non_location_pos - location_pos);
  if (non_location_pos != std::string::npos && url[non_location_pos] == '/') {
    auto non_path_pos = url.find_first_of("?#", non_location_pos + 1);
    result.path.assign(url, non_location_pos, non_path_pos - non_location_pos);
  }
  return result;
}

bool IsKnownPrefix(const std::string& prefix) { return prefix == "s3a" || prefix == "s3" || prefix == "file"; }

std::string UrlToPath(const std::string& url) {
  auto components = SplitUrl(url);
  if (IsKnownPrefix(components.schema)) {
    return components.location + components.path;
  }
  return {};
}

// clang-format off
template <typename T>
std::string SerializeValue(const T& value)
requires(std::is_same_v<bool, T> || std::is_same_v<int, T> || std::is_same_v<int64_t, T> || std::is_same_v<float, T> ||
         std::is_same_v<double, T>) {
  std::string result(sizeof(T), ' ');
  std::memcpy(result.data(), &value, sizeof(T));
  return result;
}
// clang-format on

std::string SerializeValue(const std::monostate& value) { return "null"; }

std::string SerializeValue(const std::string& value) { return value; }

std::string SerializeValue(const std::vector<uint8_t>& value) {
  std::string result(value.size(), ' ');
  std::memcpy(result.data(), value.data(), value.size());
  return result;
}

std::string SerializeValue(const DataFile::PartitionKey::Fixed& value) { return SerializeValue(value.bytes); }

std::string SerializePartitionKey(const DataFile::PartitionKey& key) {
  std::string result;
  result += key.name;
  result += key.type->ToString();
  result += std::to_string(key.value.index());
  result += std::visit([](auto&& arg) { return SerializeValue(arg); }, key.value);
  return result;
}

std::string SerializePartitionTuple(const DataFile::PartitionTuple& partition_tuple) {
  if (partition_tuple.fields.empty()) {
    return "";
  }
  std::vector<std::string> serialized_keys;
  serialized_keys.reserve(partition_tuple.fields.size());
  for (const auto& field : partition_tuple.fields) {
    serialized_keys.emplace_back(SerializePartitionKey(field));
  }

  std::string result;
  size_t total_size = 0;
  for (const auto& key : serialized_keys) {
    total_size += key.size();
    total_size += std::to_string(key.size()).size();
  }

  result.reserve(total_size);
  for (const auto& key : serialized_keys) {
    result += key;
    result += std::to_string(key.size());
  }

  return result;
}

}  // namespace

std::optional<int64_t> DataEntry::GetValueCounts(int32_t field_id) const {
  for (const auto& [id, cnt] : entry.data_file.value_counts) {
    if (id == field_id) {
      return cnt;
    }
  }
  return std::nullopt;
}

std::optional<int64_t> DataEntry::GetColumnSize(int32_t field_id) const {
  for (const auto& [id, cnt] : entry.data_file.column_sizes) {
    if (id == field_id) {
      return cnt;
    }
  }
  return std::nullopt;
}

ColumnStats DataEntry::GetColumnStats(int32_t field_id) const {
  uint64_t total_rows = entry.data_file.record_count;
  std::optional<int64_t> value_count = GetValueCounts(field_id);
  std::optional<int64_t> column_size = GetColumnSize(field_id);

  ColumnStats result{.null_count = -1,
                     .distinct_count = -1,
                     .not_null_count = -1,
                     .total_compressed_size = -1,
                     .total_uncompressed_size = -1};

  if (value_count.has_value()) {
    result.not_null_count = value_count.value();
    result.null_count = total_rows - value_count.value();
  }

  if (column_size.has_value()) {
    result.total_compressed_size = column_size.value();
    // TODO(gmusya): estimate total_uncompressed_size
  }

  return result;
}

namespace {
void Add(ColumnStats& base, const ColumnStats& addition) {
  base.distinct_count = -1;

  if (addition.not_null_count == -1 || base.not_null_count == -1) {
    base.not_null_count = -1;
  } else {
    base.not_null_count += addition.not_null_count;
  }

  if (addition.null_count == -1 || base.null_count == -1) {
    base.null_count = -1;
  } else {
    base.null_count += addition.null_count;
  }

  if (addition.total_uncompressed_size == -1 || base.total_uncompressed_size == -1) {
    base.total_uncompressed_size = -1;
  } else {
    base.total_uncompressed_size += addition.total_uncompressed_size;
  }

  if (addition.total_compressed_size == -1 || base.total_compressed_size == -1) {
    base.total_compressed_size = -1;
  } else {
    base.total_compressed_size += addition.total_compressed_size;
  }
}
}  // namespace

arrow::Result<ColumnStats> ScanMetadata::GetColumnStats(const std::string& column_name) const {
  int field_id = -1;
  {
    auto maybe_field_id = schema->FindColumnIgnoreCase(column_name);
    if (!maybe_field_id.has_value()) {
      return arrow::Status::ExecutionError("GetIcebergColumnStats: Column ", column_name, " not found in schema");
    }
    field_id = maybe_field_id.value();
  }

  ColumnStats result{.null_count = 0,
                     .distinct_count = 0,
                     .not_null_count = 0,
                     .total_compressed_size = 0,
                     .total_uncompressed_size = 0};

  for (const auto& part : partitions) {
    for (const auto& layer : part) {
      for (const auto& data_entry : layer.data_entries_) {
        ColumnStats task_stats = data_entry.GetColumnStats(field_id);
        Add(result, task_stats);
      }
    }
  }

  return result;
}

arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenFile(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                                     const std::string& url) {
  auto path = UrlToPath(url);
  if (path.empty()) {
    return ::arrow::Status::ExecutionError("bad url: ", url);
  }
  return fs->OpenInputFile(path);
}

arrow::Result<std::string> ReadFile(std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& url) {
  ARROW_ASSIGN_OR_RAISE(auto file, OpenFile(fs, url));

  std::string buffer;
  ARROW_ASSIGN_OR_RAISE(auto size, file->GetSize());
  buffer.resize(size);
  ARROW_ASSIGN_OR_RAISE(auto bytes_read, file->ReadAt(0, size, buffer.data()));
  ARROW_UNUSED(bytes_read);
  return buffer;
}

// https://iceberg.apache.org/spec/#partition-transforms
struct Transforms {
  static constexpr std::string_view kBucket = "bucket";  // actual name is bucket[X]
  static constexpr std::string_view kYear = "year";
  static constexpr std::string_view kMonth = "month";
  static constexpr std::string_view kDay = "day";
  static constexpr std::string_view kHour = "hour";

  static constexpr std::string_view kIdentity = "identity";
  static constexpr std::string_view kTruncate = "truncate";  // actual name is truncate[X]

  static constexpr std::string_view kVoid = "void";  // TODO(gmusya): support

  static arrow::Result<std::shared_ptr<const types::Type>> GetTypeFromSourceType(
      int source_id, std::shared_ptr<const iceberg::Schema> schema) {
    for (const auto& column : schema->Columns()) {
      if (column.field_id == source_id) {
        return [&]() -> std::shared_ptr<const types::Type> {
          if (column.type->TypeId() == TypeID::kUuid) {
            return std::make_shared<types::FixedType>(16);
          }
          if (column.type->TypeId() == TypeID::kTimestamptz) {
            return std::make_shared<types::PrimitiveType>(TypeID::kTimestamp);
          }
          return column.type;
        }();
      }
    }
    return arrow::Status::ExecutionError("Partition spec expects column with field id ", source_id,
                                         " for transform, but this column is missing");
  }
};

arrow::Result<ScanMetadata> GetScanMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                            const std::string& metadata_location) {
  ARROW_ASSIGN_OR_RAISE(const std::string table_metadata_content, ReadFile(fs, metadata_location));
  auto table_metadata = ice_tea::ReadTableMetadataV2(table_metadata_content);
  if (!table_metadata) {
    return arrow::Status::ExecutionError("cannot read metadata");
  }
  if (!table_metadata->current_snapshot_id.has_value()) {
    return arrow::Status::ExecutionError("no current_snapshot_id");
  }

  std::shared_ptr<Schema> schema = table_metadata->GetCurrentSchema();
  if (table_metadata->snapshots.empty()) {
    return ScanMetadata{.schema = schema};
  }

  auto partiton_spec = table_metadata->GetCurrentPartitionSpec();

  // https://iceberg.apache.org/spec/#partition-evolution
  // TODO(gmusya): support partition evolution
  if (table_metadata->partition_specs.size() != 1) {
    return arrow::Status::ExecutionError("GetScanMetadata: tables with multiple partition specs are not supported yet");
  }
  std::vector<PartitionKeyField> partition_fields;
  for (const auto& value : partiton_spec->fields) {
    const auto& transform = value.transform;
    if (transform.starts_with(Transforms::kBucket) || transform == Transforms::kYear ||
        transform == Transforms::kMonth || transform == Transforms::kDay || transform == Transforms::kHour) {
      partition_fields.emplace_back(value.name, std::make_shared<types::PrimitiveType>(TypeID::kInt));
    } else if (transform == Transforms::kIdentity || transform.starts_with(Transforms::kTruncate)) {
      ARROW_ASSIGN_OR_RAISE(auto partition_field_type, Transforms::GetTypeFromSourceType(value.source_id, schema));
      partition_fields.emplace_back(value.name, partition_field_type);
    } else {
      return arrow::Status::ExecutionError("GetScanMetadata: unexpected transform type " + transform);
    }
  }

  auto maybe_manifest_list_path = table_metadata->GetCurrentManifestListPath();
  if (!maybe_manifest_list_path.has_value()) {
    return arrow::Status::ExecutionError("no manifest_list_path");
  }
  const std::string manifest_list_path = maybe_manifest_list_path.value();

  ARROW_ASSIGN_OR_RAISE(const std::string manifest_metadatas_content, ReadFile(fs, manifest_list_path));

  std::stringstream ss(manifest_metadatas_content);
  const std::vector<ManifestFile> manifest_metadatas = ice_tea::ReadManifestList(ss);

  std::vector<DataEntry> entries_output;

  using SequenceNumber = int64_t;
  std::map<std::string, std::map<SequenceNumber, ScanMetadata::Layer>> partitions;
  // if there are k partitions and t global equality delete entries, k * t entries will be created
  // TODO(gmusya): improve
  std::map<SequenceNumber, std::vector<ManifestEntry>> global_equality_deletes;
  for (const auto& manifest_metadata : manifest_metadatas) {
    const std::string manifest_path = manifest_metadata.path;
    ARROW_ASSIGN_OR_RAISE(const std::string entries_content, ReadFile(fs, manifest_path));
    Manifest manifest = ice_tea::ReadManifestEntries(entries_content, partition_fields);
    auto& entries_input = manifest.entries;
    for (auto&& entry : entries_input) {
      if (entry.data_file.referenced_data_file.has_value()) {
        return arrow::Status::ExecutionError("Referenced data file for delete files is not supported yet");
      }
      entry.data_file.null_value_counts.clear();
      entry.data_file.nan_value_counts.clear();
      entry.data_file.distinct_counts.clear();
      entry.data_file.key_metadata.clear();

      if (entry.status == ManifestEntry::Status::kDeleted) {
        continue;
      }

      SequenceNumber sequence_number;
      if (entry.sequence_number.has_value()) {
        sequence_number = entry.sequence_number.value();
      } else if (entry.status == ManifestEntry::Status::kAdded) {
        sequence_number = manifest_metadata.sequence_number;
      } else {
        return arrow::Status::ExecutionError("no sequence_number");
      }
      entry.sequence_number = sequence_number;
      std::string serialized_partition_key = SerializePartitionTuple(entry.data_file.partition_tuple);
      switch (entry.data_file.content) {
        case ContentFile::FileContent::kData: {
          std::vector<DataEntry::Segment> segments;
          const auto& split_offsets = entry.data_file.split_offsets;
          if (split_offsets.empty()) {
            segments.emplace_back(4, 0);
          } else {
            for (size_t i = 0; i + 1 < split_offsets.size(); ++i) {
              segments.emplace_back(split_offsets[i], split_offsets[i + 1] - split_offsets[i]);
            }
            segments.emplace_back(split_offsets.back(), 0);
          }
          partitions[serialized_partition_key][sequence_number].data_entries_.emplace_back(
              DataEntry(std::move(entry), std::move(segments)));
          break;
        }
        case ContentFile::FileContent::kPositionDeletes:
          // A position delete file must be applied to a data file when all of the following are true:
          // - The data file's file_path is equal to the delete file's referenced_data_file if it is non-null
          // - The data file's data sequence number is less than or equal to the delete file's data sequence number
          // - The data file's partition (both spec and partition values) is equal [4] to the delete file's partition
          // - There is no deletion vector that must be applied to the data file (when added, such a vector must contain
          //   all deletes from existing position delete files)
          partitions[serialized_partition_key][sequence_number].positional_delete_entries_.emplace_back(
              std::move(entry));
          break;
        case ContentFile::FileContent::kEqualityDeletes:
          // An equality delete file must be applied to a data file when all of the following are true:
          // - The data file's data sequence number is strictly less than the delete's data sequence number
          // - The data file's partition (both spec id and partition values) is equal [4] to the delete file's partition
          //   or the delete file's partition spec is unpartitioned
          if (serialized_partition_key.empty()) {
            global_equality_deletes[sequence_number - 1].emplace_back(std::move(entry));
          } else {
            partitions[serialized_partition_key][sequence_number - 1].equality_delete_entries_.emplace_back(
                std::move(entry));
          }
          break;
      }
    }
  }

  ScanMetadata result;
  result.schema = schema;

  for (auto& [partition_key, partition_map] : partitions) {
    for (const auto& [seqnum, equality_delete_entries] : global_equality_deletes) {
      auto& deletes_at_current_layer = partition_map[seqnum].equality_delete_entries_;
      deletes_at_current_layer.insert(deletes_at_current_layer.end(), equality_delete_entries.begin(),
                                      equality_delete_entries.end());
    }
  }

  for (auto& [partition_key, partition_map] : partitions) {
    ScanMetadata::Partition partition;
    for (auto& [seqnum, layer] : partition_map) {
      partition.emplace_back(std::move(layer));
    }
    result.partitions.emplace_back(std::move(partition));
  }

  return result;
}

}  // namespace iceberg::ice_tea
