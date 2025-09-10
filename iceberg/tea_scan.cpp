#include "iceberg/tea_scan.h"

#include <chrono>
#include <deque>
#include <exception>
#include <iterator>
#include <limits>
#include <memory>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>

#include "arrow/filesystem/filesystem.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/common/error.h"
#include "iceberg/common/logger.h"
#include "iceberg/common/threadpool.h"
#include "iceberg/experimental_representations.h"
#include "iceberg/filter/representation/value.h"
#include "iceberg/filter/stats_filter/stats.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_entry_stats_getter.h"
#include "iceberg/manifest_file.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transforms.h"
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

std::vector<ManifestFile> GetManifestFiles(std::shared_ptr<arrow::fs::FileSystem> fs,
                                           const std::string& manifest_list_path) {
  const std::string manifest_metadatas_content = ValueSafe(ReadFile(fs, manifest_list_path));

  std::stringstream ss(manifest_metadatas_content);
  return ice_tea::ReadManifestList(ss);
}

void AddSequenceNumberOrFail(const ManifestFile& manifest, ManifestEntry& entry) {
  if (!entry.sequence_number.has_value() && entry.status == ManifestEntry::Status::kAdded) {
    entry.sequence_number = manifest.sequence_number;
  }
  Ensure(entry.sequence_number.has_value(),
         "No sequence_number in iceberg::ManifestEntry for data file " + entry.data_file.file_path);
}

int ConvertToInt(const std::vector<uint8_t>& value) {
  Ensure(value.size() == sizeof(int), std::string(__PRETTY_FUNCTION__) + ": byte array size doesn't match");
  int res;
  std::memcpy(&res, value.data(), sizeof(int));
  return res;
}

int YearsToDays(int years) {
  constexpr int kMaxDaysPerYear = 366;
  Ensure(years <= std::numeric_limits<int>::max() / kMaxDaysPerYear &&
             years >= std::numeric_limits<int>::min() / kMaxDaysPerYear,
         std::string(__PRETTY_FUNCTION__) + ": days can overflow");
  return std::chrono::sys_days{std::chrono::year(1970 + years) / 1 / 1}.time_since_epoch().count();
}

int MonthsToDays(int months) {
  constexpr int kMaxDaysPerMonth = 31;
  Ensure(months <= std::numeric_limits<int>::max() / kMaxDaysPerMonth &&
             months >= std::numeric_limits<int>::min() / kMaxDaysPerMonth,
         std::string(__PRETTY_FUNCTION__) + ": days can overflow");
  Ensure(months >= 0, std::string(__PRETTY_FUNCTION__) + ": handling negative months is not supported yet");
  return std::chrono::sys_days{std::chrono::year(1970 + months / 12) / std::chrono::month(1 + months % 12) / 1}
      .time_since_epoch()
      .count();
#if 0
    int32_t abs_months = -months;
    int32_t years_to_sub = (11 + abs_months) / 12;
    int32_t months_to_sub = abs_months % 12;
    int32_t result_year = 1970 - years_to_sub;
    int32_t result_month = months_to_sub == 0 ? 1 : (13 - months_to_sub);
    return std::chrono::sys_days{std::chrono::year(result_year) / (result_month) / 1}.time_since_epoch().count();
#endif
}

int DaysToHours(int days) {
  constexpr int kHoursPerDay = 24;
  Ensure(
      days <= std::numeric_limits<int>::max() / kHoursPerDay && days >= std::numeric_limits<int>::min() / kHoursPerDay,
      std::string(__PRETTY_FUNCTION__) + ": hours will overflow");
  return days * kHoursPerDay;
}

int64_t HoursToMicros(int64_t hours) {
  constexpr int64_t kMicrosPerHour = 3600ll * 1000ll * 1000ll;
  Ensure(hours <= std::numeric_limits<int64_t>::max() / kMicrosPerHour &&
             hours >= std::numeric_limits<int64_t>::min() / kMicrosPerHour,
         std::string(__PRETTY_FUNCTION__) + ": micros will overflow");
  return hours * kMicrosPerHour;
}

class ManifestFileStatsGetter : public filter::IStatsGetter {
 public:
  ManifestFileStatsGetter(const std::unordered_map<std::string, TypeID>& name_to_type,
                          std::unordered_map<std::string, int>&& name_to_position,
                          const std::vector<PartitionFieldSummary>& partitions, std::shared_ptr<PartitionSpec> spec)
      : name_to_type_(name_to_type),
        name_to_position_(std::move(name_to_position)),
        partitions_(partitions),
        spec_(spec) {
    Ensure(spec != nullptr, std::string(__PRETTY_FUNCTION__) + ": spec is nullptr");
  }

  std::optional<filter::GenericStats> GetStats(const std::string& column_name,
                                               filter::ValueType value_type) const override {
    auto it = name_to_position_.find(column_name);
    if (it == name_to_position_.end()) {
      return std::nullopt;
    }
    const auto position = it->second;
    Ensure(position >= 0 && position < partitions_.size(), std::string(__PRETTY_FUNCTION__) + ": invalid position");

    const auto& transform = spec_->fields[position].transform;
    if (transform == identity_transform_prefix) {
      Ensure(name_to_type_.contains(column_name),
             std::string(__PRETTY_FUNCTION__) + ": type for column '" + column_name + "' is not found");
      const TypeID ice_type = name_to_type_.at(column_name);

      return GetStatsFromIdentityTransform(ice_type, value_type, position);
    } else if (transform == year_transform_prefix || transform == month_transform_prefix ||
               transform == day_transform_prefix || transform == hour_transform_prefix) {
      return GetStatsFromTemporalTransform(value_type, position);
    } else {
      return std::nullopt;
    }
  }

 private:
  std::optional<filter::GenericStats> GetStatsFromIdentityTransform(TypeID ice_type, filter::ValueType value_type,
                                                                    int position) const {
    auto maybe_conversion = TypesToStatsConverter(ice_type, value_type);
    if (!maybe_conversion) {
      return std::nullopt;
    }

    auto minmax_after_transform =
        (*maybe_conversion)(partitions_[position].lower_bound, partitions_[position].upper_bound);

    if (!minmax_after_transform.has_value()) {
      return std::nullopt;
    }

    return filter::GenericStats(std::move(*minmax_after_transform));
  }

  static std::optional<std::pair<int32_t, int32_t>> GetMinMaxDayFromTemporal(int32_t min_partition_value,
                                                                             int32_t max_partition_value,
                                                                             const std::string& transform) {
    if (transform == year_transform_prefix) {
      // note that it is important to add 1 to max_partition_values, because `year = 0` means that days can be from
      // 0 to 365
      int32_t min_day = YearsToDays(min_partition_value);
      int32_t max_day = YearsToDays(max_partition_value + 1) - 1;
      return std::make_pair(min_day, max_day);
    } else if (transform == month_transform_prefix) {
      int32_t min_day = MonthsToDays(min_partition_value);
      int32_t max_day = MonthsToDays(max_partition_value + 1) - 1;
      return std::make_pair(min_day, max_day);
    } else if (transform == day_transform_prefix) {
      int32_t min_day = min_partition_value;
      int32_t max_day = max_partition_value;
      return std::make_pair(min_day, max_day);
    } else {
      return std::nullopt;
    }
  }

  static std::optional<filter::GenericStats> GetStatsFromTemporalForTimestamp(int32_t min_partition_value,
                                                                              int32_t max_partition_value,
                                                                              const std::string& transform,
                                                                              bool is_timestamptz) {
    int64_t micros_min;
    int64_t micros_max;
    if (transform == hour_transform_prefix) {
      micros_min = HoursToMicros(min_partition_value);
      micros_max = HoursToMicros(max_partition_value + 1) - 1;
    } else {
      std::optional<std::pair<int32_t, int32_t>> min_max_day =
          GetMinMaxDayFromTemporal(min_partition_value, max_partition_value, transform);
      Ensure(min_max_day.has_value(), std::string(__PRETTY_FUNCTION__) + ": internal error");

      int64_t min_hour = DaysToHours(min_max_day->first);
      int64_t max_hour = DaysToHours(min_max_day->second + 1) - 1;
      micros_min = HoursToMicros(min_hour);
      micros_max = HoursToMicros(max_hour + 1) - 1;
    }

    if (is_timestamptz) {
      // transform(timestamptz) actually means transform(castTimestamp(timestamptz))
      // timestamptz (utc) and timestamp can differ no more than by 15 hours (utc-14 ... utc+12), so we
      // are pessimistic here
      const int64_t kMicrosInDay = HoursToMicros(DaysToHours(1));
      if (micros_min < std::numeric_limits<int64_t>::min() + kMicrosInDay) {
        return std::nullopt;
      }
      if (micros_max > std::numeric_limits<int64_t>::max() - kMicrosInDay) {
        return std::nullopt;
      }
      return filter::GenericStats(filter::GenericMinMaxStats::Make<filter::ValueType::kTimestamptz>(
          micros_min - kMicrosInDay, micros_max + kMicrosInDay));
    } else {
      return filter::GenericStats(
          filter::GenericMinMaxStats::Make<filter::ValueType::kTimestamp>(micros_min, micros_max));
    }
  }

  static std::optional<filter::GenericStats> GetStatsFromTemporalForDate(int32_t min_partition_value,
                                                                         int32_t max_partition_value,
                                                                         const std::string& transform) {
    std::optional<std::pair<int32_t, int32_t>> min_max_day =
        GetMinMaxDayFromTemporal(min_partition_value, max_partition_value, transform);
    Ensure(min_max_day.has_value(), std::string(__PRETTY_FUNCTION__) + ": internal error");

    return filter::GenericStats(
        filter::GenericMinMaxStats::Make<filter::ValueType::kDate>(min_max_day->first, min_max_day->second));
  }

  std::optional<filter::GenericStats> GetStatsFromTemporalTransform(filter::ValueType value_type, int position) const {
    int32_t min_partition_value = ConvertToInt(partitions_[position].lower_bound);
    int32_t max_partition_value = ConvertToInt(partitions_[position].upper_bound);
    if (max_partition_value == std::numeric_limits<int32_t>::max()) {
      // not supported
      return std::nullopt;
    }
    // real partition values are in [min_partition_value; max_partition_value]

    const std::string& transform = spec_->fields[position].transform;

    if (value_type == filter::ValueType::kDate) {
      return GetStatsFromTemporalForDate(min_partition_value, max_partition_value, transform);
    } else if (value_type == filter::ValueType::kTimestamp || value_type == filter::ValueType::kTimestamptz) {
      return GetStatsFromTemporalForTimestamp(min_partition_value, max_partition_value, transform,
                                              value_type == filter::ValueType::kTimestamptz);
    } else {
      return std::nullopt;
    }
  }

  const std::unordered_map<std::string, TypeID>& name_to_type_;
  std::unordered_map<std::string, int> name_to_position_;
  const std::vector<PartitionFieldSummary>& partitions_;
  std::shared_ptr<PartitionSpec> spec_;
};

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

static Manifest GetManifest(std::shared_ptr<arrow::fs::FileSystem> fs, const ManifestFile& manifest_file,
                            std::shared_ptr<iceberg::Schema> schema,
                            const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs, bool use_reader_schema,
                            const ManifestEntryDeserializerConfig& config) {
  const std::string entries_content = ValueSafe(ReadFile(fs, manifest_file.path));

  auto maybe_partition_spec = GetFieldsFromPartitionSpec(*partition_specs.at(manifest_file.partition_spec_id), schema);
  Ensure(maybe_partition_spec.has_value(), std::string(__PRETTY_FUNCTION__) + ": failed to parse partition_spec_id " +
                                               std::to_string(manifest_file.partition_spec_id));
  return ice_tea::ReadManifestEntries(entries_content, maybe_partition_spec.value(), config, use_reader_schema);
}

static std::shared_ptr<IcebergEntriesStream> GetManifestEntryStream(
    std::shared_ptr<arrow::fs::FileSystem> fs, const ManifestFile& manifest_file,
    std::shared_ptr<iceberg::Schema> schema, const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs,
    bool use_reader_schema, const ManifestEntryDeserializerConfig& config) {
  std::string entries_content = ValueSafe(ReadFile(fs, manifest_file.path));

  auto maybe_partition_spec = GetFieldsFromPartitionSpec(*partition_specs.at(manifest_file.partition_spec_id), schema);
  Ensure(maybe_partition_spec.has_value(), std::string(__PRETTY_FUNCTION__) + ": failed to parse partition_spec_id " +
                                               std::to_string(manifest_file.partition_spec_id));
  return ice_tea::make::ManifestEntriesStream(std::move(entries_content), maybe_partition_spec.value(), config,
                                              use_reader_schema);
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

std::vector<bool> FilterManifests(std::shared_ptr<filter::StatsFilter> stats_filter,
                                  std::shared_ptr<iceberg::Schema> schema,
                                  const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs,
                                  const std::vector<ManifestFile>& manifest_metadatas) {
  std::unordered_map<int, int> spec_id_to_position;
  for (int i = 0; i < partition_specs.size(); ++i) {
    spec_id_to_position[partition_specs[i]->spec_id] = i;
  }

  std::unordered_map<int, std::string> field_id_to_name;
  std::unordered_map<std::string, TypeID> name_to_type;

  const std::unordered_set<std::string_view> allowed_transforms = {
      Transforms::kIdentity, Transforms::kHour, Transforms::kDay, Transforms::kMonth, Transforms::kYear};

  for (const auto& column : schema->Columns()) {
    field_id_to_name[column.field_id] = column.name;
    name_to_type[column.name] = column.type->TypeId();
  }

  std::vector<bool> can_skip(manifest_metadatas.size(), false);
  for (size_t i = 0; i < manifest_metadatas.size(); ++i) {
    const auto& manifest = manifest_metadatas[i];
    auto partition_spec = [&]() {
      auto it = spec_id_to_position.find(manifest.partition_spec_id);
      Ensure(it != spec_id_to_position.end(), std::string(__PRETTY_FUNCTION__) + ": partition spec id is not found");
      return partition_specs[it->second];
    }();

    bool only_allowed_transforms = true;
    for (const auto& field : partition_spec->fields) {
      only_allowed_transforms &= allowed_transforms.contains(field.transform);
    }

    if (!only_allowed_transforms) {
      continue;
    }

    std::unordered_map<std::string, int> name_to_position;
    for (size_t i = 0; i < partition_spec->fields.size(); ++i) {
      const auto& field = partition_spec->fields[i];
      auto it = field_id_to_name.find(field.source_id);
      Ensure(it != field_id_to_name.end(), std::string(__PRETTY_FUNCTION__) + ": partition source id is not found");
      name_to_position[it->second] = i;
    }

    ManifestFileStatsGetter stats_getter(name_to_type, std::move(name_to_position), manifest.partitions,
                                         partition_spec);

    can_skip[i] = stats_filter->ApplyFilter(stats_getter) == filter::MatchedRows::kNone;
  }
  return can_skip;
}

std::shared_ptr<AllEntriesStream> AllEntriesStream::Make(
    std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& manifest_list_path, bool use_reader_schema,
    const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs, std::shared_ptr<iceberg::Schema> schema,
    std::shared_ptr<filter::StatsFilter> stats_filter, const ManifestEntryDeserializerConfig& config) {
  std::vector<ManifestFile> manifest_metadatas = GetManifestFiles(fs, manifest_list_path);

  std::queue<ManifestFile> manifest_files_queue = [&]() {
    try {
      if (stats_filter != nullptr && schema != nullptr) {
        std::vector<bool> can_skip = FilterManifests(stats_filter, schema, partition_specs, manifest_metadatas);
        std::queue<ManifestFile> result;
        for (size_t i = 0; i < manifest_metadatas.size(); ++i) {
          if (!can_skip.at(i)) {
            result.push(manifest_metadatas[i]);
          }
        }
        return result;
      }
    } catch (std::exception& e) {
      // if we failed to apply filter than fallback to parsing metadata completely
      // TODO(gmusya): add logging
    } catch (arrow::Status& e) {
    }
    return std::queue<ManifestFile>(std::deque<ManifestFile>(std::make_move_iterator(manifest_metadatas.begin()),
                                                             std::make_move_iterator(manifest_metadatas.end())));
  }();

  return std::make_shared<AllEntriesStream>(fs, std::move(manifest_files_queue), use_reader_schema, partition_specs,
                                            schema, config);
}

std::shared_ptr<AllEntriesStream> AllEntriesStream::Make(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                         std::shared_ptr<TableMetadataV2> table_metadata,
                                                         bool use_reader_schema,
                                                         std::shared_ptr<filter::StatsFilter> stats_filter,
                                                         const ManifestEntryDeserializerConfig& config) {
  const std::string manifest_list_path = table_metadata->GetCurrentManifestListPathOrFail();

  return AllEntriesStream::Make(fs, manifest_list_path, use_reader_schema, table_metadata->partition_specs,
                                table_metadata->GetCurrentSchema(), stats_filter, config);
}

class EntryStream : public IcebergEntriesStream {
 public:
  EntryStream(std::shared_ptr<arrow::fs::FileSystem> fs, const ManifestFile& manifest_file,
              std::shared_ptr<iceberg::Schema> schema,
              const std::vector<std::shared_ptr<PartitionSpec>>& partition_specs, bool use_reader_schema,
              const ManifestEntryDeserializerConfig& config)
      : manifest_file_(manifest_file) {
    all_stream_ = GetManifestEntryStream(fs, manifest_file, schema, partition_specs, use_reader_schema, config);
  }

  std::optional<ManifestEntry> ReadNext() override {
    while (true) {
      std::optional<ManifestEntry> maybe_result = all_stream_->ReadNext();
      if (!maybe_result) {
        return std::nullopt;
      }

      ManifestEntry result = std::move(*maybe_result);
      AddSequenceNumberOrFail(manifest_file_, result);

      if (result.status == ManifestEntry::Status::kDeleted) {
        continue;
      }

      return result;
    }
  }

 private:
  const ManifestFile manifest_file_;
  std::shared_ptr<IcebergEntriesStream> all_stream_;
};

std::optional<ManifestEntry> AllEntriesStream::ReadNext() {
  while (true) {
    if (!current_manifest_stream_) {
      if (manifest_files_.empty()) {
        return std::nullopt;
      }

      ManifestFile current_manifest_file = std::move(manifest_files_.front());
      manifest_files_.pop();

      current_manifest_stream_ = std::make_shared<EntryStream>(fs_, current_manifest_file, schema_, partition_specs_,
                                                               use_avro_reader_schema_, config_);
    }

    std::optional<ManifestEntry> maybe_result = current_manifest_stream_->ReadNext();
    if (!maybe_result) {
      current_manifest_stream_.reset();
      continue;
    }

    return maybe_result;
  }
}

arrow::Result<ScanMetadata> GetScanMetadataMultiThreaded(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                         std::shared_ptr<TableMetadataV2> table_metadata,
                                                         bool use_reader_schema, uint32_t threads_num,
                                                         const ManifestEntryDeserializerConfig& config,
                                                         std::shared_ptr<ILogger> logger);

arrow::Result<ScanMetadata> GetScanMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                            const std::string& metadata_location,
                                            std::function<bool(iceberg::Schema& schema)> use_avro_reader_schema,
                                            std::shared_ptr<filter::StatsFilter> stats_filter, uint32_t threads_num,
                                            const GetScanMetadataConfig& config, std::shared_ptr<ILogger> logger) {
  auto data = ValueSafe(ReadFile(fs, metadata_location));
  std::shared_ptr<TableMetadataV2> table_metadata = ReadTableMetadataV2(data);
  if (!table_metadata) {
    return arrow::Status::ExecutionError("GetScanMetadata: failed to parse metadata " + metadata_location);
  }
  if (!table_metadata->GetCurrentSchema()) {
    return arrow::Status::ExecutionError("GetScanMetadata: failed to parse metadata " + metadata_location +
                                         " (failed to get schema)");
  }
  if (threads_num == 0) {
    auto entries_stream =
        AllEntriesStream::Make(fs, table_metadata, use_avro_reader_schema(*table_metadata->GetCurrentSchema()),
                               stats_filter, config.manifest_entry_deserializer_config);
    return GetScanMetadata(*entries_stream, *table_metadata, logger);
  }
  return GetScanMetadataMultiThreaded(fs, table_metadata, use_avro_reader_schema(*table_metadata->GetCurrentSchema()),
                                      threads_num, config.manifest_entry_deserializer_config, logger);
}

class ScanMetadataBuilder {
 public:
  explicit ScanMetadataBuilder(const TableMetadataV2& table_metadata, std::shared_ptr<ILogger> logger)
      : table_metadata_(table_metadata), schema_(table_metadata_.GetCurrentSchema()), logger_(std::move(logger)) {}

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

      std::optional<std::string> min_data_path;
      std::optional<std::string> max_data_path;

      for (auto it = partition_map.rbegin(); it != partition_map.rend(); ++it) {
        auto& [seqno, layer] = *it;

        for (const auto& data_entry : layer.data_entries_) {
          if (!min_data_path.has_value() || *min_data_path > data_entry.path) {
            min_data_path = data_entry.path;
          }
          if (!max_data_path.has_value() || *max_data_path < data_entry.path) {
            max_data_path = data_entry.path;
          }
        }

        ScanMetadata::Layer result_layer;
        result_layer.data_entries_ = std::move(layer.data_entries_);
        result_layer.equality_delete_entries_ = std::move(result_layer.equality_delete_entries_);

        int64_t dangling_positional_delete_files = 0;
        for (const auto& pos_delete : layer.positional_delete_entries_) {
          bool has_stats =
              pos_delete.min_max_referenced_path_.has_value() && min_data_path.has_value() && max_data_path.has_value();
          if (has_stats) {
            const auto& [min_referenced_path, max_referenced_path] = *pos_delete.min_max_referenced_path_;
            if (*min_data_path > max_referenced_path || *max_data_path < min_referenced_path) {
              ++dangling_positional_delete_files;
              continue;
            }
          }
          result_layer.positional_delete_entries_.emplace_back(std::move(pos_delete.positional_delete_.path));
        }

        if (logger_) {
          if (dangling_positional_delete_files > 0) {
            logger_->Log(std::to_string(dangling_positional_delete_files), "metrics:plan:dangling_positional_files");
          }
          if (result_layer.data_entries_.size() > 0) {
            logger_->Log(std::to_string(result_layer.data_entries_.size()), "metrics:plan:data_files");
          }
          if (result_layer.positional_delete_entries_.size() > 0) {
            logger_->Log(std::to_string(result_layer.positional_delete_entries_.size()),
                         "metrics:plan:positional_files");
          }
          if (result_layer.equality_delete_entries_.size() > 0) {
            logger_->Log(std::to_string(result_layer.equality_delete_entries_.size()), "metrics:plan:equality_files");
          }
        }

        if (!result_layer.data_entries_.empty() || !result_layer.positional_delete_entries_.empty() ||
            !result_layer.equality_delete_entries_.empty()) {
          partition.emplace_back(std::move(result_layer));
        }
      }

      std::reverse(partition.begin(), partition.end());
      if (min_data_path.has_value()) {
        result.partitions.emplace_back(std::move(partition));
      }
    }

    return result;
  }

  arrow::Status AddEntry(const iceberg::ManifestEntry& entry) {
    if (entry.status == ManifestEntry::Status::kDeleted) {
      return arrow::Status::OK();
    }

    ARROW_RETURN_NOT_OK(CheckPartitionTupleIsCorrect(entry));
    std::string serialized_partition_key = SerializePartitionTuple(entry.data_file.partition_tuple);

    StoreEntry(std::move(serialized_partition_key), entry);

    return arrow::Status::OK();
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
        AddDataFile(serialized_partition_key, sequence_number, entry.data_file.file_path, std::move(segments));
        break;
      }
      case ContentFile::FileContent::kPositionDeletes: {
        // A position delete file must be applied to a data file when all of the following are true:
        // - The data file's file_path is equal to the delete file's referenced_data_file if it is non-null
        // - The data file's data sequence number is less than or equal to the delete file's data sequence number
        // - The data file's partition (both spec and partition values) is equal [4] to the delete file's partition
        // - There is no deletion vector that must be applied to the data file (when added, such a vector must
        // contain
        //   all deletes from existing position delete files)
        std::optional<std::pair<std::string, std::string>> min_max_referenced_path;
        constexpr uint32_t kFilePathId = 2147483546;
        if (entry.data_file.lower_bounds.contains(kFilePathId) && entry.data_file.upper_bounds.contains(kFilePathId)) {
          const std::vector<uint8_t>& min_bytes = entry.data_file.lower_bounds.at(kFilePathId);
          const std::vector<uint8_t>& max_bytes = entry.data_file.upper_bounds.at(kFilePathId);

          std::string min_path(min_bytes.begin(), min_bytes.end());
          std::string max_path(max_bytes.begin(), max_bytes.end());

          min_max_referenced_path.emplace(std::move(min_path), std::move(max_path));
        }
        AddPositionDeletes(serialized_partition_key, sequence_number, entry.data_file.file_path,
                           min_max_referenced_path);
        break;
      }

      case ContentFile::FileContent::kEqualityDeletes:
        // An equality delete file must be applied to a data file when all of the following are true:
        // - The data file's data sequence number is strictly less than the delete's data sequence number
        // - The data file's partition (both spec id and partition values) is equal [4] to the delete file's
        // partition
        //   or the delete file's partition spec is unpartitioned
        if (serialized_partition_key.empty()) {
          AddGlobalEqualityDeletes(sequence_number, entry.data_file.file_path, entry.data_file.equality_ids);
        } else {
          AddEqualityDeletes(serialized_partition_key, sequence_number, entry.data_file.file_path,
                             entry.data_file.equality_ids);
        }
        break;
    }
  }

 protected:
  virtual void AddDataFile(const std::string& serialized_partition_key, SequenceNumber sequence_number,
                           const std::string& path, std::vector<DataEntry::Segment>&& segments) {
    partitions[serialized_partition_key][sequence_number].data_entries_.emplace_back(
        DataEntry(path, std::move(segments)));
  }

  virtual void AddPositionDeletes(const std::string& serialized_partition_key, SequenceNumber sequence_number,
                                  const std::string& path,
                                  const std::optional<std::pair<std::string, std::string>>& min_max_referenced_path) {
    partitions[serialized_partition_key][sequence_number].positional_delete_entries_.emplace_back(
        path, min_max_referenced_path);
  }

  virtual void AddGlobalEqualityDeletes(SequenceNumber sequence_number, const std::string& path,
                                        const std::vector<int>& equality_ids) {
    global_equality_deletes[sequence_number - 1].emplace_back(path, equality_ids);
  }

  virtual void AddEqualityDeletes(const std::string& serialized_partition_key, SequenceNumber sequence_number,
                                  const std::string& path, const std::vector<int>& equality_ids) {
    partitions[serialized_partition_key][sequence_number - 1].equality_delete_entries_.emplace_back(path, equality_ids);
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

  struct PositionalDeleteWithExtraInfo {
    PositionalDeleteInfo positional_delete_;
    std::optional<std::pair<std::string, std::string>> min_max_referenced_path_;

    PositionalDeleteWithExtraInfo(std::string path,
                                  std::optional<std::pair<std::string, std::string>> min_max_referenced_path)
        : positional_delete_(std::move(path)), min_max_referenced_path_(std::move(min_max_referenced_path)) {}
  };

  struct LayerWithExtraInfo {
    std::vector<DataEntry> data_entries_;
    std::vector<PositionalDeleteWithExtraInfo> positional_delete_entries_;
    std::vector<EqualityDeleteInfo> equality_delete_entries_;

    bool operator==(const LayerWithExtraInfo& layer) const = default;

    bool Empty() const;
  };

  std::map<std::string, std::map<SequenceNumber, LayerWithExtraInfo>> partitions;
  // if there are k partitions and t global equality delete entries, k * t entries will be created
  // TODO(gmusya): improve
  std::map<SequenceNumber, std::vector<EqualityDeleteInfo>> global_equality_deletes;
  std::shared_ptr<ILogger> logger_;
};

class ScanMetadataBuilderMT : public ScanMetadataBuilder {
 public:
  explicit ScanMetadataBuilderMT(const TableMetadataV2& table_metadata, std::shared_ptr<ILogger> logger)
      : ScanMetadataBuilder(table_metadata, logger) {}

 private:
  void AddDataFile(const std::string& serialized_partition_key, SequenceNumber sequence_number, const std::string& path,
                   std::vector<DataEntry::Segment>&& segments) override {
    std::lock_guard<std::mutex> guard(mutex_);
    ScanMetadataBuilder::AddDataFile(serialized_partition_key, sequence_number, path, std::move(segments));
  }

  void AddPositionDeletes(const std::string& serialized_partition_key, SequenceNumber sequence_number,
                          const std::string& path,
                          const std::optional<std::pair<std::string, std::string>>& min_max_referenced_path) override {
    std::lock_guard<std::mutex> guard(mutex_);
    ScanMetadataBuilder::AddPositionDeletes(serialized_partition_key, sequence_number, path, min_max_referenced_path);
  }

  void AddGlobalEqualityDeletes(SequenceNumber sequence_number, const std::string& path,
                                const std::vector<int>& equality_ids) override {
    std::lock_guard<std::mutex> guard(mutex_);
    ScanMetadataBuilder::AddGlobalEqualityDeletes(sequence_number, path, equality_ids);
  }

  void AddEqualityDeletes(const std::string& serialized_partition_key, SequenceNumber sequence_number,
                          const std::string& path, const std::vector<int>& equality_ids) override {
    std::lock_guard<std::mutex> guard(mutex_);
    ScanMetadataBuilder::AddEqualityDeletes(serialized_partition_key, sequence_number, path, equality_ids);
  }

  // consider making separate mutexes for each variable
  // partitions_mutex_, global_equality_deletes_mutex_
  std::mutex mutex_;
};

class ReferencedDataFileAwareScanPlanner {
 public:
  ReferencedDataFileAwareScanPlanner(const TableMetadataV2& table_metadata, std::shared_ptr<ILogger> logger)
      : builder_(std::make_shared<ScanMetadataBuilder>(table_metadata, std::move(logger))) {}

  arrow::Status AddEntry(const iceberg::ManifestEntry& entry) {
    if (entry.status == iceberg::ManifestEntry::Status::kDeleted) {
      return arrow::Status::OK();
    }

    switch (entry.data_file.content) {
      case iceberg::DataFile::FileContent::kData:
        break;
      case iceberg::DataFile::FileContent::kEqualityDeletes: {
        SetHasEqualityDelete();
        break;
      }
      case iceberg::DataFile::FileContent::kPositionDeletes:
        if (entry.data_file.referenced_data_file.has_value()) {
          ARROW_RETURN_NOT_OK(HandlePositionDelete(entry));
        } else {
          SetAllPosDeletes();
        }
        break;
    }
    return builder_->AddEntry(entry);
  }

  arrow::Result<ScanMetadata> GetResult() {
    ScanMetadata meta = builder_->GetResult();

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

 protected:
  ReferencedDataFileAwareScanPlanner(std::shared_ptr<ScanMetadataBuilder> builder) : builder_(builder) {}

  virtual void SetHasEqualityDelete() { has_equality_delete = true; }
  virtual void SetAllPosDeletes() { all_pos_deletes_annotated_with_referenced_data_file = true; }

  virtual arrow::Status HandlePositionDelete(const ManifestEntry& entry) {
    has_referenced_data_file = true;
    if (delete_file_path_to_referenced_data_file_path_.contains(entry.data_file.file_path)) {
      return arrow::Status::ExecutionError("Two delete files with same path: " + entry.data_file.file_path);
    }
    delete_file_path_to_referenced_data_file_path_[entry.data_file.file_path] =
        entry.data_file.referenced_data_file.value();
    return arrow::Status::OK();
  }

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

  bool all_pos_deletes_annotated_with_referenced_data_file = true;
  bool has_equality_delete = false;
  bool has_referenced_data_file = false;

 private:
  std::shared_ptr<ScanMetadataBuilder> builder_;
};

class ReferencedDataFileAwareScanPlannerMT : public ReferencedDataFileAwareScanPlanner {
 public:
  explicit ReferencedDataFileAwareScanPlannerMT(const TableMetadataV2& table_metadata, std::shared_ptr<ILogger> logger)
      : ReferencedDataFileAwareScanPlanner(std::make_shared<ScanMetadataBuilderMT>(table_metadata, logger)) {}

 private:
  void SetHasEqualityDelete() override {
    std::lock_guard<std::mutex> guard(mutex_);
    ReferencedDataFileAwareScanPlanner::SetHasEqualityDelete();
  }

  void SetAllPosDeletes() override {
    std::lock_guard<std::mutex> guard(mutex_);
    ReferencedDataFileAwareScanPlanner::SetAllPosDeletes();
  }

  arrow::Status HandlePositionDelete(const ManifestEntry& entry) override {
    std::lock_guard<std::mutex> guard(mutex_);
    return ReferencedDataFileAwareScanPlanner::HandlePositionDelete(entry);
  }

  // consider making separate mutexes for each variable
  // has_equality_delete_mutex_, referenced_data_file_mutex_, all_pos_deletes_mutex_;
  std::mutex mutex_;
};

arrow::Result<ScanMetadata> GetScanMetadata(IcebergEntriesStream& entries_stream, const TableMetadataV2& table_metadata,
                                            std::shared_ptr<ILogger> logger) {
  ReferencedDataFileAwareScanPlanner scan_metadata_builder(table_metadata, logger);

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

class ManifestEntryTask {
 public:
  ManifestEntryTask(iceberg::ManifestEntry&& entry,
                    std::shared_ptr<ReferencedDataFileAwareScanPlannerMT> scan_metadata_builder)
      : entry_(std::move(entry)), scan_metadata_builder_(scan_metadata_builder) {}

  arrow::Status operator()() { return scan_metadata_builder_->AddEntry(entry_); }

 private:
  iceberg::ManifestEntry entry_;
  std::shared_ptr<ReferencedDataFileAwareScanPlannerMT> scan_metadata_builder_;
};

class ManifestFileTask {
 public:
  ManifestFileTask(std::shared_ptr<arrow::fs::FileSystem> fs, ManifestFile&& manifest_file,
                   std::shared_ptr<ThreadPool> pool, std::shared_ptr<TableMetadataV2> table_metadata,
                   std::shared_ptr<ReferencedDataFileAwareScanPlannerMT> scan_metadata_builder, bool use_reader_schema,
                   const ManifestEntryDeserializerConfig& config)
      : fs_(fs),
        manifest_file_(std::move(manifest_file)),
        pool_(pool),
        table_metadata_(table_metadata),
        scan_metadata_builder_(scan_metadata_builder),
        use_reader_schema_(use_reader_schema),
        config_(config) {}

  std::vector<std::future<arrow::Status>> operator()() {
    Manifest manifest = GetManifest(fs_, manifest_file_, table_metadata_->GetCurrentSchema(),
                                    table_metadata_->partition_specs, use_reader_schema_, config_);
    std::vector<std::future<arrow::Status>> statuses(manifest.entries.size());
    for (size_t i = 0; i < manifest.entries.size(); ++i) {
      AddSequenceNumberOrFail(manifest_file_, manifest.entries[i]);
      statuses[i] = pool_->Submit(ManifestEntryTask(std::move(manifest.entries[i]), scan_metadata_builder_));
    }
    return statuses;
  }

 private:
  std::shared_ptr<arrow::fs::FileSystem> fs_;
  ManifestFile manifest_file_;
  std::shared_ptr<ThreadPool> pool_;
  std::shared_ptr<TableMetadataV2> table_metadata_;
  std::shared_ptr<ReferencedDataFileAwareScanPlannerMT> scan_metadata_builder_;
  bool use_reader_schema_;
  ManifestEntryDeserializerConfig config_;
};

arrow::Result<ScanMetadata> GetScanMetadataMultiThreaded(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                         std::shared_ptr<TableMetadataV2> table_metadata,
                                                         bool use_reader_schema, uint32_t threads_num,
                                                         const ManifestEntryDeserializerConfig& config,
                                                         std::shared_ptr<ILogger> logger) {
  const std::string manifest_list_path = table_metadata->GetCurrentManifestListPathOrFail();
  auto manifest_metadatas = GetManifestFiles(fs, manifest_list_path);

  auto scan_metadata_builder = std::make_shared<ReferencedDataFileAwareScanPlannerMT>(*table_metadata, logger);

  auto pool = std::make_shared<ThreadPool>(threads_num);
  std::vector<std::future<std::vector<std::future<arrow::Status>>>> futures;
  futures.reserve(manifest_metadatas.size());

  for (auto& manifest : manifest_metadatas) {
    futures.emplace_back(pool->Submit(ManifestFileTask(fs, std::move(manifest), pool, table_metadata,
                                                       scan_metadata_builder, use_reader_schema, config)));
  }

  std::vector<std::vector<std::future<arrow::Status>>> statuses;
  statuses.reserve(manifest_metadatas.size());

  for (auto& future : futures) {
    statuses.emplace_back(future.get());
  }

  for (auto& manifest_statuses : statuses) {
    for (auto& status : manifest_statuses) {
      ARROW_RETURN_NOT_OK(status.get());
    }
  }
  return scan_metadata_builder->GetResult();
}

}  // namespace iceberg::ice_tea
