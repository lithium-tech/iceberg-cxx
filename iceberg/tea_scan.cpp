#include "iceberg/tea_scan.h"

#include <deque>
#include <iterator>
#include <memory>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

#include "arrow/filesystem/filesystem.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/common/error.h"
#include "iceberg/experimental_representations.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
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
  if (key.type) {
    result += key.type->ToString();
  }
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

DataEntry operator+(const DataEntry& lhs, const DataEntry& rhs) {
  Ensure(lhs.path == rhs.path, "Pathes of left and right entries are not equals: " + lhs.path + ", " + rhs.path);

  std::vector<DataEntry::Segment> all_segments = lhs.parts;
  for (const auto& elem : rhs.parts) {
    all_segments.push_back(elem);
  }
  std::sort(all_segments.begin(), all_segments.end(),
            [](const auto& lhs, const auto& rhs) { return lhs.offset < rhs.offset; });
  all_segments = experimental::UniteSegments(all_segments);
  return DataEntry(lhs.path, all_segments);
}

DataEntry& DataEntry::operator+=(const DataEntry& other) {
  Ensure(path == other.path, "Pathes of left and right entries are not equals: " + path + ", " + other.path);

  for (const auto& elem : other.parts) {
    parts.push_back(elem);
  }
  std::sort(parts.begin(), parts.end(), [](const auto& lhs, const auto& rhs) { return lhs.offset < rhs.offset; });
  parts = experimental::UniteSegments(parts);
  return *this;
}

bool ScanMetadata::Layer::Empty() const {
  return data_entries_.empty() && positional_delete_entries_.empty() && equality_delete_entries_.empty();
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

  static constexpr std::string_view kVoid = "void";

  static arrow::Result<std::shared_ptr<const types::Type>> GetTypeFromSourceType(
      int source_id, std::shared_ptr<const iceberg::Schema> schema) {
    for (const auto& column : schema->Columns()) {
      if (column.field_id == source_id) {
        return [&]() -> std::shared_ptr<const types::Type> {
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

static std::optional<std::vector<PartitionKeyField>> GetFieldsFromPartitionSpec(
    const PartitionSpec& partition_spec, std::shared_ptr<const iceberg::Schema> schema) {
  std::vector<PartitionKeyField> partition_fields;
  for (const auto& value : partition_spec.fields) {
    const auto& transform = value.transform;
    if (transform.starts_with(Transforms::kBucket) || transform == Transforms::kYear ||
        transform == Transforms::kMonth || transform == Transforms::kHour) {
      partition_fields.emplace_back(value.name, std::make_shared<types::PrimitiveType>(TypeID::kInt));
    } else if (transform == Transforms::kDay) {
      // https://iceberg.apache.org/spec/#partition-transforms
      // see https://github.com/apache/iceberg/pull/11749
      partition_fields.emplace_back(value.name, std::make_shared<types::PrimitiveType>(TypeID::kDate));
    } else if (transform == Transforms::kIdentity || transform.starts_with(Transforms::kTruncate)) {
      auto maybe_partition_field_type = Transforms::GetTypeFromSourceType(value.source_id, schema);
      if (!maybe_partition_field_type.ok()) {
        return std::nullopt;
      }
      partition_fields.emplace_back(value.name, maybe_partition_field_type.MoveValueUnsafe());
    } else if (transform == Transforms::kVoid) {
      partition_fields.emplace_back(value.name, nullptr);
    } else {
      return std::nullopt;
    }
  }

  return partition_fields;
}

static bool MatchesSpecification(const PartitionSpec& partition_spec, std::shared_ptr<const iceberg::Schema> schema,
                                 const ContentFile::PartitionTuple& tuple) {
  std::optional<std::vector<PartitionKeyField>> partition_fields = GetFieldsFromPartitionSpec(partition_spec, schema);
  if (!partition_fields) {
    return false;
  }

  if (partition_fields->size() != tuple.fields.size()) {
    return false;
  }

  for (const auto& expected_field : *partition_fields) {
    bool is_found = false;
    for (const auto& actual_field : tuple.fields) {
      if (actual_field.name == expected_field.name) {
        if (is_found) {
          // multiple fields with one name is unexpected
          return false;
        }

        is_found = true;
        if (actual_field.type && expected_field.type &&
            actual_field.type->ToString() != expected_field.type->ToString()) {
          return false;
        }

        bool is_void_transform = expected_field.type == nullptr;
        bool result_is_null = std::holds_alternative<std::monostate>(actual_field.value);
        if (is_void_transform && !result_is_null) {
          return false;
        }
      }
    }

    if (!is_found) {
      return false;
    }
  }

  return true;
}

using SequenceNumber = int64_t;

std::shared_ptr<AllEntriesStream> AllEntriesStream::Make(
    std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& manifest_list_path, bool use_reader_schema,
    const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs, std::shared_ptr<iceberg::Schema> schema,
    const ManifestEntryDeserializerConfig& config) {
  const std::string manifest_metadatas_content = ValueSafe(ReadFile(fs, manifest_list_path));

  std::stringstream ss(manifest_metadatas_content);
  std::vector<ManifestFile> manifest_metadatas = ice_tea::ReadManifestList(ss);
  std::queue<ManifestFile> manifest_files_queue(std::deque<ManifestFile>(
      std::make_move_iterator(manifest_metadatas.begin()), std::make_move_iterator(manifest_metadatas.end())));

  return std::make_shared<AllEntriesStream>(fs, std::move(manifest_files_queue), use_reader_schema, partition_specs,
                                            schema, config);
}

std::shared_ptr<AllEntriesStream> AllEntriesStream::Make(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                         std::shared_ptr<TableMetadataV2> table_metadata,
                                                         bool use_reader_schema,
                                                         const ManifestEntryDeserializerConfig& config) {
  auto maybe_manifest_list_path = table_metadata->GetCurrentManifestListPath();
  Ensure(maybe_manifest_list_path.has_value(), "MakeIcebergEntriesStream: manifest list path is not found");

  const std::string manifest_list_path = maybe_manifest_list_path.value();

  return AllEntriesStream::Make(fs, manifest_list_path, use_reader_schema, table_metadata->partition_specs,
                                table_metadata->GetCurrentSchema(), config);
}

std::optional<ManifestEntry> AllEntriesStream::ReadNext() {
  while (true) {
    if (!entries_for_current_manifest_file_.empty()) {
      auto entry = std::move(entries_for_current_manifest_file_.front());
      entries_for_current_manifest_file_.pop();

      if (!entry.sequence_number.has_value() && entry.status == ManifestEntry::Status::kAdded) {
        entry.sequence_number = current_manifest_file.sequence_number;
      }
      Ensure(entry.sequence_number.has_value(),
             "No sequence_number in iceberg::ManifestEntry for data file " + entry.data_file.file_path);

      if (entry.status == ManifestEntry::Status::kDeleted) {
        continue;
      }

      return entry;
    }

    if (manifest_files_.empty()) {
      return std::nullopt;
    }

    current_manifest_file = std::move(manifest_files_.front());
    manifest_files_.pop();

    const std::string manifest_path = current_manifest_file.path;
    const std::string entries_content = ValueSafe(ReadFile(fs_, manifest_path));

    auto maybe_partition_spec =
        GetFieldsFromPartitionSpec(*partition_specs_.at(current_manifest_file.partition_spec_id), schema_);
    Ensure(maybe_partition_spec.has_value(), std::string(__PRETTY_FUNCTION__) + ": failed to parse partition_spec_id " +
                                                 std::to_string(current_manifest_file.partition_spec_id));

    Manifest manifest =
        ice_tea::ReadManifestEntries(entries_content, maybe_partition_spec.value(), config_, use_avro_reader_schema_);
    // it is impossible to construct queue from iterators before C++23
    entries_for_current_manifest_file_ = std::queue<ManifestEntry>(std::deque<ManifestEntry>(
        std::make_move_iterator(manifest.entries.begin()), std::make_move_iterator(manifest.entries.end())));
  }
}

arrow::Result<ScanMetadata> GetScanMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                            const std::string& metadata_location,
                                            std::function<bool(iceberg::Schema& schema)> use_avro_reader_schema,
                                            const GetScanMetadataConfig& config) {
  auto data = ValueSafe(ReadFile(fs, metadata_location));
  std::shared_ptr<TableMetadataV2> table_metadata = ReadTableMetadataV2(data);
  if (!table_metadata) {
    return arrow::Status::ExecutionError("GetScanMetadata: failed to parse metadata " + metadata_location);
  }
  if (!table_metadata->GetCurrentSchema()) {
    return arrow::Status::ExecutionError("GetScanMetadata: failed to parse metadata " + metadata_location +
                                         " (failed to get schema)");
  }
  auto entries_stream =
      AllEntriesStream::Make(fs, table_metadata, use_avro_reader_schema(*table_metadata->GetCurrentSchema()),
                             config.manifest_entry_deserializer_config);
  return GetScanMetadata(*entries_stream, *table_metadata);
}

class ScanMetadataBuilder {
 public:
  explicit ScanMetadataBuilder(const TableMetadataV2& table_metadata)
      : table_metadata_(table_metadata), schema_(table_metadata_.GetCurrentSchema()) {}

  arrow::Status AddEntry(const iceberg::ManifestEntry& entry) {
    if (entry.status == ManifestEntry::Status::kDeleted) {
      return arrow::Status::OK();
    }

    ARROW_RETURN_NOT_OK(CheckPartitionTupleIsCorrect(entry));
    std::string serialized_partition_key = SerializePartitionTuple(entry.data_file.partition_tuple);

    StoreEntry(std::move(serialized_partition_key), entry);

    return arrow::Status::OK();
  }

  ScanMetadata GetResult() {
    ScanMetadata result;
    result.schema = table_metadata_.GetCurrentSchema();

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

 private:
  void StoreEntry(std::string serialized_partition_key, const iceberg::ManifestEntry& entry) {
    SequenceNumber sequence_number = entry.sequence_number.value();

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
            DataEntry(std::move(entry.data_file.file_path), std::move(segments)));
        break;
      }
      case ContentFile::FileContent::kPositionDeletes:
        // A position delete file must be applied to a data file when all of the following are true:
        // - The data file's file_path is equal to the delete file's referenced_data_file if it is non-null
        // - The data file's data sequence number is less than or equal to the delete file's data sequence number
        // - The data file's partition (both spec and partition values) is equal [4] to the delete file's partition
        // - There is no deletion vector that must be applied to the data file (when added, such a vector must
        // contain
        //   all deletes from existing position delete files)
        partitions[serialized_partition_key][sequence_number].positional_delete_entries_.emplace_back(
            std::move(entry.data_file.file_path));
        break;
      case ContentFile::FileContent::kEqualityDeletes:
        // An equality delete file must be applied to a data file when all of the following are true:
        // - The data file's data sequence number is strictly less than the delete's data sequence number
        // - The data file's partition (both spec id and partition values) is equal [4] to the delete file's
        // partition
        //   or the delete file's partition spec is unpartitioned
        if (serialized_partition_key.empty()) {
          global_equality_deletes[sequence_number - 1].emplace_back(std::move(entry.data_file.file_path),
                                                                    std::move(entry.data_file.equality_ids));
        } else {
          partitions[serialized_partition_key][sequence_number - 1].equality_delete_entries_.emplace_back(
              std::move(entry.data_file.file_path), std::move(entry.data_file.equality_ids));
        }
        break;
    }
  }

  arrow::Status CheckPartitionTupleIsCorrect(const iceberg::ManifestEntry& entry) const {
    // https://iceberg.apache.org/docs/1.7.1/evolution/
    // We do not know from which partition the current entry is
    size_t matching_specifications = 0;
    const auto& specs = table_metadata_.partition_specs;
    for (size_t i = 0; i < specs.size(); ++i) {
      // Matching also should use field-id from partition specification and field-id (as custom attribute) from
      // avro, but avrocpp does not currently support custom attributes
      if (MatchesSpecification(*specs[i], schema_, entry.data_file.partition_tuple)) {
        ++matching_specifications;
      }
    }

    if (matching_specifications == 0) {
      return arrow::Status::ExecutionError("Partiton specification for entry ", entry.data_file.file_path,
                                           " is not found");
    }
    if (matching_specifications > 1) {
      return arrow::Status::ExecutionError("Multiple (", matching_specifications,
                                           ") partiton specifications for entry ", entry.data_file.file_path,
                                           " are found");
    }

    return arrow::Status::OK();
  }

  const TableMetadataV2& table_metadata_;
  std::shared_ptr<const iceberg::Schema> schema_;

  std::map<std::string, std::map<SequenceNumber, ScanMetadata::Layer>> partitions;
  // if there are k partitions and t global equality delete entries, k * t entries will be created
  // TODO(gmusya): improve
  std::map<SequenceNumber, std::vector<EqualityDeleteInfo>> global_equality_deletes;
};

class ReferencedDataFileAwareScanPlanner {
 public:
  explicit ReferencedDataFileAwareScanPlanner(const TableMetadataV2& table_metadata)
      : default_scan_metadata_builder_(table_metadata) {}

  arrow::Status AddEntry(const iceberg::ManifestEntry& entry) {
    if (entry.status == iceberg::ManifestEntry::Status::kDeleted) {
      return arrow::Status::OK();
    }

    switch (entry.data_file.content) {
      case iceberg::DataFile::FileContent::kData:
        break;
      case iceberg::DataFile::FileContent::kEqualityDeletes:
        has_equality_delete = true;
        break;
      case iceberg::DataFile::FileContent::kPositionDeletes:
        if (entry.data_file.referenced_data_file.has_value()) {
          has_referenced_data_file = true;
          if (delete_file_path_to_referenced_data_file_path_.contains(entry.data_file.file_path)) {
            return arrow::Status::ExecutionError("Two delete files with same path: " + entry.data_file.file_path);
          }
          delete_file_path_to_referenced_data_file_path_[entry.data_file.file_path] =
              entry.data_file.referenced_data_file.value();
        } else {
          all_pos_deletes_annotated_with_referenced_data_file = false;
        }
        break;
    }

    return default_scan_metadata_builder_.AddEntry(entry);
  }

  arrow::Result<ScanMetadata> GetResult() {
    ScanMetadata meta = default_scan_metadata_builder_.GetResult();

    {
      // there is nothing to optimize
      if (!has_referenced_data_file) {
        return meta;
      }

      // cannot optimize with current interface
      // also optimizing case with both positional and equality deletes seems unnecessary
      if (has_equality_delete) {
        return meta;
      }

      // it is possible to optimize in this case, but it seems as rare case
      if (!all_pos_deletes_annotated_with_referenced_data_file) {
        return meta;
      }
    }

    // now we know that:
    // * all deletes are positional
    // * all deletes are annotated with reference data file

    ScanMetadata result;
    result.schema = meta.schema;

    // data without deletes are stored in 0-th layer of 0-th partition
    // other partitions will contain exactly one data file and all deletes for this file (in one layer)
    {
      ScanMetadata::Layer layer{};
      ScanMetadata::Partition partition;
      partition.emplace_back(std::move(layer));
      result.partitions.emplace_back(std::move(partition));
    }

    for (auto&& part : meta.partitions) {
      ARROW_RETURN_NOT_OK(HandlePartition(std::move(part), result));
    }

    // if there are no data files without deletes, then 0-th partitions is empty
    // remove empty partition from result
    if (result.partitions[0][0].data_entries_.empty()) {
      result.partitions.erase(result.partitions.begin());
    }

    return result;
  }

 private:
  arrow::Status HandlePartition(ScanMetadata::Partition&& part, ScanMetadata& result) {
    using LayerNumber = uint32_t;

    std::map<std::string, std::pair<DataEntry, LayerNumber>> data_file_name_to_data_entry_;
    std::map<std::string, std::vector<std::pair<PositionalDeleteInfo, LayerNumber>>> data_file_name_to_posdel_entries_;

    for (LayerNumber layer_no = 0; layer_no < part.size(); ++layer_no) {
      auto&& layer = std::move(part[layer_no]);
      for (auto&& data_entry : layer.data_entries_) {
        std::string path = data_entry.path;

        if (data_file_name_to_data_entry_.contains(path)) {
          return arrow::Status::ExecutionError(
              "Error in iceberg metadata. There are at least two data entries with path ", path);
        }
        data_file_name_to_data_entry_.emplace(std::move(path), std::make_pair(std::move(data_entry), layer_no));
      }

      for (auto&& del_entry : layer.positional_delete_entries_) {
        if (!delete_file_path_to_referenced_data_file_path_.contains(del_entry.path)) {
          return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": internal error for delete ", del_entry.path);
        }
        std::string data_path = delete_file_path_to_referenced_data_file_path_.at(del_entry.path);
        data_file_name_to_posdel_entries_[data_path].emplace_back(std::move(del_entry), layer_no);
      }
    }

    for (auto&& [data_path, data_entry_with_layer_no] : data_file_name_to_data_entry_) {
      auto& [data_entry, data_layer_no] = data_entry_with_layer_no;
      if (!data_file_name_to_posdel_entries_.contains(data_path)) {
        result.partitions[0][0].data_entries_.emplace_back(std::move(data_entry));
        continue;
      }

      result.partitions.emplace_back();
      auto& new_partition = result.partitions.back();
      new_partition.emplace_back();
      auto& new_layer = new_partition.back();

      new_layer.data_entries_.emplace_back(std::move(data_entry));

      // ignore deletes with sequence number less than sequence number in data file
      auto& deletes = data_file_name_to_posdel_entries_.at(data_path);
      for (auto& [del, del_layer_no] : deletes) {
        if (del_layer_no < data_layer_no) {
          continue;
        }

        new_layer.positional_delete_entries_.emplace_back(std::move(del));
      }
    }

    return arrow::Status::OK();
  }

  std::map<std::string, std::string> delete_file_path_to_referenced_data_file_path_;
  ScanMetadataBuilder default_scan_metadata_builder_;

  bool all_pos_deletes_annotated_with_referenced_data_file = true;
  bool has_equality_delete = false;
  bool has_referenced_data_file = false;
};

arrow::Result<ScanMetadata> GetScanMetadata(IcebergEntriesStream& entries_stream,
                                            const TableMetadataV2& table_metadata) {
  ReferencedDataFileAwareScanPlanner scan_metadata_builder(table_metadata);

  while (true) {
    std::optional<ManifestEntry> maybe_entry = entries_stream.ReadNext();
    if (!maybe_entry.has_value()) {
      break;
    }

    ManifestEntry entry = std::move(maybe_entry.value());
    ARROW_RETURN_NOT_OK(scan_metadata_builder.AddEntry(entry));
  }

  return scan_metadata_builder.GetResult();
}

}  // namespace iceberg::ice_tea
