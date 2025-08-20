#include "iceberg/manifest_entry.h"

#include <LogicalType.hh>
#include <Node.hh>
#include <cstdint>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Generic.hh"
#include "avro/Schema.hh"
#include "avro/Types.hh"
#include "avro/ValidSchema.hh"
#include "iceberg/avro_schema.h"
#include "iceberg/type.h"
#include "parquet/arrow/reader.h"
#include "parquet/metadata.h"
#include "parquet/statistics.h"
#include "rapidjson/document.h"

namespace iceberg {
using NodePtr = std::shared_ptr<iceavro::Node>;

std::vector<int64_t> SplitOffsets(std::shared_ptr<parquet::FileMetaData> parquet_meta) {
  std::vector<int64_t> split_offsets;
  split_offsets.reserve(parquet_meta->num_row_groups());
  for (int i = 0; i < parquet_meta->num_row_groups(); ++i) {
    auto rg_meta = parquet_meta->RowGroup(i);
    Ensure(rg_meta != nullptr, "No row group for id " + std::to_string(i));
    split_offsets.push_back(rg_meta->file_offset());
  }
  std::sort(split_offsets.begin(), split_offsets.end());
  return split_offsets;
}

std::shared_ptr<parquet::FileMetaData> ParquetMetadata(std::shared_ptr<arrow::io::RandomAccessFile> input_file) {
  parquet::arrow::FileReaderBuilder reader_builder;
  auto status = reader_builder.Open(input_file, parquet::default_reader_properties());
  Ensure(status.ok(), "cannot open parquet file");

  reader_builder.memory_pool(arrow::default_memory_pool());
  auto maybe_arrow_reader = reader_builder.Build();
  Ensure(maybe_arrow_reader.ok(), maybe_arrow_reader.status().message());

  auto arrow_reader = maybe_arrow_reader.MoveValueUnsafe();
  return arrow_reader->parquet_reader()->metadata();
}

std::shared_ptr<parquet::FileMetaData> ParquetMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                       const std::string& file_path, uint64_t& file_size) {
  auto input_file = fs->OpenInputFile(file_path);
  Ensure(input_file.ok(), "Cannot open input file: " + file_path);
  auto size_rez = (*input_file)->GetSize();
  Ensure(size_rez.ok(), "Cannot get input file size: " + file_path);
  file_size = *size_rez;
  return ParquetMetadata(*input_file);
}

namespace ice_tea {

namespace {

template <typename T>
T Deserialize(const avro::GenericDatum& datum);

// clang-format off
template <typename T>
T Deserialize(const avro::GenericDatum& datum)
requires(std::is_same_v<std::vector<uint8_t>, T>);

template <typename T>
T Deserialize(const avro::GenericDatum& datum)
requires(!std::is_same_v<std::vector<uint8_t>, T> && std::is_same_v<std::vector<typename T::value_type>, T>);

template <typename T>
T Deserialize(const avro::GenericDatum& datum)
requires(std::is_same_v<std::optional<typename T::value_type>, T>);

template <typename T>
T Deserialize(const avro::GenericDatum& datum)
requires(std::is_same_v<std::pair<typename T::first_type, typename T::second_type>, T>);

template <typename T>
T Deserialize(const avro::GenericDatum& datum)
requires(std::is_same_v<std::map<typename T::key_type, typename T::value_type::second_type>, T>);

class ExtractError : public std::runtime_error {
  public:
  explicit ExtractError(const std::string& message) : std::runtime_error(message) {}
}; 

template <typename T>
T Extract(const avro::GenericDatum& datum, const std::string& name)
requires(std::is_same_v<std::vector<typename T::value_type>, T> ||
         std::is_same_v<std::optional<typename T::value_type>, T> ||
         std::is_same_v<std::map<typename T::key_type, typename T::value_type::second_type>, T>);
// clang-format on

template <typename T>
T Extract(const avro::GenericRecord& datum, const std::string& name) {
  try {
    const auto& field = datum.field(name);
    return Deserialize<T>(field);
  } catch (const ExtractError& e) {
    throw;
  } catch (const std::exception& e) {
    if (std::string(e.what()).starts_with("Invalid field name:")) {
      throw ExtractError(std::string(__PRETTY_FUNCTION__) + ": field '" + name + "' is missing");
    } else {
      throw ExtractError(std::string(__PRETTY_FUNCTION__) + ": " + e.what());
    }
  }
}

// clang-format off
template <typename T>
T Extract(const avro::GenericRecord& datum, const std::string& name)
requires(std::is_same_v<std::vector<typename T::value_type>, T> ||
         std::is_same_v<std::optional<typename T::value_type>, T> ||
         std::is_same_v<std::map<typename T::key_type, typename T::value_type::second_type>, T>)
{
  bool invalid_field_name = false;
  try {
    const auto& field = datum.field(name);
  return Deserialize<T>(field);
  } catch (const ExtractError& e) {
    throw;
  } catch (const std::exception& e) {
    if (std::string(e.what()).starts_with("Invalid field name:")) {
      invalid_field_name = true;
    }
  }
  if (invalid_field_name) {
    return T{};
  }
  throw ExtractError("Undefined Avro error in " + std::string(__PRETTY_FUNCTION__));
}

template <typename T>
T Deserialize(const avro::GenericDatum& datum)
requires(std::is_same_v<std::vector<uint8_t>, T>)
{
  if (datum.type() != avro::AVRO_BYTES) {
    return std::vector<uint8_t>{};
  }
  return datum.value<std::vector<uint8_t>>();
}

template <typename T>
T Deserialize(const avro::GenericDatum& datum)
requires(!std::is_same_v<std::vector<uint8_t>, T> && std::is_same_v<std::vector<typename T::value_type>, T>)
{
  if (datum.type() != avro::AVRO_ARRAY) {
    return T{};
  } else {
    T result;
    const auto& array = datum.value<avro::GenericArray>();
    result.reserve(array.value().size());
    for (const auto& elem : array.value()) {
      result.emplace_back(Deserialize<typename T::value_type>(elem));
    }
    return result;
  }
}

template <typename T>
T Deserialize(const avro::GenericDatum& datum)
requires(std::is_same_v<std::optional<typename T::value_type>, T>)
{
  if (datum.type() == avro::AVRO_NULL) {
    return std::nullopt;
  } else {
    return Deserialize<typename T::value_type>(datum);
  }
}

template <typename T>
T Deserialize(const avro::GenericDatum& datum)
requires(std::is_same_v<std::pair<typename T::first_type, typename T::second_type>, T>)
{
  Ensure(datum.type() == avro::AVRO_RECORD, std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");
  const auto& record = datum.value<avro::GenericRecord>();
  auto first = Extract<typename T::first_type>(record, "key");
  auto second = Extract<typename T::second_type>(record, "value");
  return std::make_pair(std::move(first), std::move(second));
}

template <typename T>
T Deserialize(const avro::GenericDatum& datum)
requires(std::is_same_v<std::map<typename T::key_type, typename T::value_type::second_type>, T>)
{
  auto vec = Deserialize<std::vector<std::pair<typename T::key_type, typename T::value_type::second_type>>>(datum);
  T result;
  for (auto&& [key, value] : vec) {
    result.emplace(key, std::move(value));
  }
  return result;
}
// clang-format on

template <>
int Deserialize(const avro::GenericDatum& datum) {
  Ensure(datum.type() == avro::AVRO_INT, std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");
  return datum.value<int>();
}

template <>
int64_t Deserialize(const avro::GenericDatum& datum) {
  Ensure(datum.type() == avro::AVRO_LONG, std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");
  return datum.value<int64_t>();
}

template <>
std::string Deserialize(const avro::GenericDatum& datum) {
  Ensure(datum.type() == avro::AVRO_STRING, std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");
  return datum.value<std::string>();
}

template <>
DataFile::PartitionTuple Deserialize(const avro::GenericDatum& datum) {
  Ensure(datum.type() == avro::AVRO_RECORD, std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");

  const auto& record = datum.value<avro::GenericRecord>();
  DataFile::PartitionTuple result;
  const auto& schema = record.schema();
  for (size_t i = 0; i < schema->names(); ++i) {
    auto name = schema->nameAt(i);

    const auto& field = record.fieldAt(record.fieldIndex(name));
    const auto logical_type = field.logicalType();

    if (field.type() == avro::AVRO_NULL) {
      result.fields.emplace_back(name);
      continue;
    }

    if (field.type() == avro::AVRO_INT) {
      const int value = field.value<int>();
      switch (logical_type.type()) {
        case avro::LogicalType::NONE:
          result.fields.emplace_back(name, value, std::make_shared<types::PrimitiveType>(TypeID::kInt));
          break;
        case avro::LogicalType::DATE:
          result.fields.emplace_back(name, value, std::make_shared<types::PrimitiveType>(TypeID::kDate));
          break;
        default:
          throw std::runtime_error("Unexpected logical type in avro for int: " +
                                   std::to_string(static_cast<int>(logical_type.type())));
      }

      continue;
    }

    if (field.type() == avro::AVRO_LONG) {
      const int64_t value = field.value<int64_t>();
      switch (logical_type.type()) {
        case avro::LogicalType::NONE:
          result.fields.emplace_back(name, value, std::make_shared<types::PrimitiveType>(TypeID::kLong));
          break;
        case avro::LogicalType::TIMESTAMP_MICROS:
          result.fields.emplace_back(name, value, std::make_shared<types::PrimitiveType>(TypeID::kTimestamp));
          break;
        case avro::LogicalType::TIMESTAMP_NANOS:
          result.fields.emplace_back(name, value, std::make_shared<types::PrimitiveType>(TypeID::kTimestampNs));
          break;
        case avro::LogicalType::TIME_MICROS:
          result.fields.emplace_back(name, value, std::make_shared<types::PrimitiveType>(TypeID::kTime));
          break;
        default:
          throw std::runtime_error("Unexpected logical type in avro for long: " +
                                   std::to_string(static_cast<int>(logical_type.type())));
      }

      continue;
    }

    if (field.type() == avro::AVRO_FIXED) {
      auto generic_fixed = field.value<avro::GenericFixed>();
      auto fixed = DataFile::PartitionKey::Fixed{.bytes = field.value<avro::GenericFixed>().value()};

      if (logical_type.type() == avro::LogicalType::DECIMAL) {
        result.fields.emplace_back(
            name, std::move(fixed),
            std::make_shared<types::DecimalType>(logical_type.precision(), logical_type.scale()));
      } else if (logical_type.type() == avro::LogicalType::UUID) {
        result.fields.emplace_back(name, std::move(fixed), std::make_shared<types::PrimitiveType>(TypeID::kUuid));
      } else {
        result.fields.emplace_back(name, std::move(fixed), std::make_shared<types::FixedType>(fixed.bytes.size()));
      }

      continue;
    }

    Ensure(logical_type.type() == avro::LogicalType::NONE,
           "Unexpected logical type in avro: " + std::to_string(static_cast<int>(logical_type.type())));

    if (field.type() == avro::AVRO_STRING) {
      result.fields.emplace_back(name, field.value<std::string>(),
                                 std::make_shared<types::PrimitiveType>(TypeID::kString));
    } else if (field.type() == avro::AVRO_BOOL) {
      result.fields.emplace_back(name, field.value<bool>(), std::make_shared<types::PrimitiveType>(TypeID::kBoolean));
    } else if (field.type() == avro::AVRO_BYTES) {
      result.fields.emplace_back(name, field.value<std::vector<uint8_t>>(),
                                 std::make_shared<types::PrimitiveType>(TypeID::kBinary));
    } else if (field.type() == avro::AVRO_FLOAT) {
      result.fields.emplace_back(name, field.value<float>(), std::make_shared<types::PrimitiveType>(TypeID::kFloat));
    } else if (field.type() == avro::AVRO_DOUBLE) {
      result.fields.emplace_back(name, field.value<double>(), std::make_shared<types::PrimitiveType>(TypeID::kDouble));
    } else {
      throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": unexpected avro type " +
                               std::to_string(static_cast<int>(field.type())));
    }
  }

  std::sort(result.fields.begin(), result.fields.end(),
            [&](const auto& lhs, const auto& rhs) { return lhs.name < rhs.name; });
  return result;
}

template <typename T>
void ExtractIf(const avro::GenericRecord& datum, const std::string& name, T& value, bool cond) {
  if (cond) {
    value = Extract<T>(datum, name);
  }
}

class DataFileDeserializer {
 public:
  DataFileDeserializer(const DataFileDeserializerConfig& config) : config_(config) {}

  DataFile Deserialize(const avro::GenericDatum& datum) {
    Ensure(datum.type() == avro::AVRO_RECORD, std::string(__PRETTY_FUNCTION__) + ": data.type() is not record");

    const auto& record = datum.value<avro::GenericRecord>();

    DataFile data_file{};
    ExtractIf<std::string>(record, "file_path", data_file.file_path, config_.extract_file_path);

    if (config_.extract_content) {
      data_file.content = static_cast<DataFile::FileContent>(Extract<int>(record, "content"));
    }
    ExtractIf<std::string>(record, "file_format", data_file.file_format, config_.extract_file_format);
    ExtractIf<int64_t>(record, "record_count", data_file.record_count, config_.extract_record_count);
    ExtractIf<int64_t>(record, "file_size_in_bytes", data_file.file_size_in_bytes, config_.extract_file_size_in_bytes);
    ExtractIf<std::optional<int32_t>>(record, "sort_order_id", data_file.sort_order_id, config_.extract_sort_order_id);
    ExtractIf<std::optional<std::string>>(record, "referenced_data_file", data_file.referenced_data_file,
                                          config_.extract_referenced_data_file);
    ExtractIf<std::map<int32_t, int64_t>>(record, "column_sizes", data_file.column_sizes, config_.extract_column_sizes);
    ExtractIf<std::map<int32_t, int64_t>>(record, "value_counts", data_file.value_counts, config_.extract_value_counts);
    ExtractIf<std::vector<int64_t>>(record, "split_offsets", data_file.split_offsets, config_.extract_split_offsets);
    ExtractIf<std::map<int32_t, int64_t>>(record, "null_value_counts", data_file.null_value_counts,
                                          config_.extract_null_value_counts);
    ExtractIf<std::map<int32_t, std::vector<uint8_t>>>(record, "lower_bounds", data_file.lower_bounds,
                                                       config_.extract_lower_bounds);
    ExtractIf<std::map<int32_t, std::vector<uint8_t>>>(record, "upper_bounds", data_file.upper_bounds,
                                                       config_.extract_upper_bounds);
    ExtractIf<DataFile::PartitionTuple>(record, "partition", data_file.partition_tuple,
                                        config_.extract_partition_tuple);
    ExtractIf<std::map<int32_t, int64_t>>(record, "nan_value_counts", data_file.nan_value_counts,
                                          config_.extract_nan_value_counts);
    ExtractIf<std::map<int32_t, int64_t>>(record, "distinct_counts", data_file.distinct_counts,
                                          config_.extract_distinct_counts);
    // TODO(gmusya): this part is not covered by tests in iceberg-cxx. Fix
    ExtractIf<std::vector<int32_t>>(record, "equality_ids", data_file.equality_ids, config_.extract_equality_ids);
    return data_file;
  }

 private:
  DataFileDeserializerConfig config_;
};

class ManifestEntryDeserializer {
 public:
  ManifestEntryDeserializer(const ManifestEntryDeserializerConfig& config) : config_(config) {}

  ManifestEntry Deserialize(const avro::GenericDatum& datum) {
    Ensure(datum.type() == avro::AVRO_RECORD, std::string(__PRETTY_FUNCTION__) + ": datum.type() is not record");

    const auto& record = datum.value<avro::GenericRecord>();

    ManifestEntry entry = [&]() {
      if (record.hasField("data_file")) {
        const auto& field = record.field("data_file");
        DataFileDeserializer deserializer(config_.datafile_config);
        return ManifestEntry{.data_file = deserializer.Deserialize(field)};
      }
      return ManifestEntry{};
    }();
    if (config_.extract_status) {
      entry.status = static_cast<ManifestEntry::Status>(Extract<int>(record, "status"));
    }
    ExtractIf<std::optional<int64_t>>(record, "snapshot_id", entry.snapshot_id, config_.extract_snapshot_id);
    ExtractIf<std::optional<int64_t>>(record, "sequence_number", entry.sequence_number,
                                      config_.extract_sequence_number);
    ExtractIf<std::optional<int64_t>>(record, "file_sequence_number", entry.file_sequence_number,
                                      config_.extract_file_sequence_number);
    return entry;
  }

 private:
  ManifestEntryDeserializerConfig config_;
};

NodePtr MakeSchemaPartition(const std::vector<PartitionKeyField>& partition_spec) {
  // TODO(gmusya): support field ids
  auto result = std::make_shared<iceavro::RecordNode>("r102", 102);
  for (const auto& field : partition_spec) {
    switch (field.type->TypeId()) {
      case TypeID::kBoolean:
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::BoolNode>()));
        break;
      case TypeID::kInt:
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::IntNode>()));
        break;
      case TypeID::kLong:
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::LongNode>()));
        break;
      case TypeID::kDate:
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::DateNode>()));
        break;
      case TypeID::kTime:
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::TimeNode>()));
        break;
      case TypeID::kTimestamp:
        result->AddField(field.name,
                         std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::TimestampNode>()));
        break;
      case TypeID::kTimestamptz:
        result->AddField(field.name,
                         std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::TimestamptzNode>()));
        break;
      case TypeID::kTimestampNs:
        result->AddField(field.name,
                         std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::TimestampNsNode>()));
        break;
      case TypeID::kTimestamptzNs:
        result->AddField(field.name,
                         std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::TimestamptzNsNode>()));
        break;
      case TypeID::kDecimal: {
        auto decimal_type = std::static_pointer_cast<const types::DecimalType>(field.type);
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::DecimalNode>(
                                         decimal_type->Precision(), decimal_type->Scale())));
        break;
      }
      case TypeID::kFixed: {
        auto fixed_type = std::static_pointer_cast<const types::FixedType>(field.type);
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(
                                         std::make_shared<iceavro::FixedNode>(fixed_type->Size())));
        break;
      }
      case TypeID::kUuid:
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::UuidNode>()));
        break;
      case TypeID::kString:
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::StringNode>()));
        break;
      case TypeID::kBinary:
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::BytesNode>()));
        break;
      case TypeID::kFloat:
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::FloatNode>()));
        break;
      case TypeID::kDouble:
        result->AddField(field.name, std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::DoubleNode>()));
        break;
      default:
        throw std::runtime_error("Unexpected data iceberg::TypeID in partitioning: " +
                                 std::to_string(static_cast<int>(field.type->TypeId())));
    }
  }
  return result;
}

// TODO(gmusya): use field id
#if 0
namespace field_id {
constexpr int32_t kContent = 134;
constexpr int32_t kFilePath = 100;
constexpr int32_t kFileFormat = 101;
constexpr int32_t kPartition = 102;
constexpr int32_t kRecordCount = 103;
constexpr int32_t kFileSizeInBytes = 104;
constexpr int32_t kColumnSizes = 108;
constexpr int32_t kValueCounts = 109;
constexpr int32_t kNullValueCounts = 110;
constexpr int32_t kDistinctCounts = 111;
constexpr int32_t kLowerBounds = 125;
constexpr int32_t kUpperBounds = 128;
constexpr int32_t kKeyMetadata = 131;
constexpr int32_t kSplitOffsets = 132;
constexpr int32_t kEqualityIds = 135;
constexpr int32_t kNanValueCounts = 137;
constexpr int32_t kSortOrderId = 140;
constexpr int32_t kReferencedDataFile = 143;
}  // namespace field_id
#endif

// Spec (https://iceberg.apache.org/spec/#avro) says
// <<Optional fields, array elements, and map values must be wrapped in an Avro union with null.
// This is the only union type allowed in Iceberg data files.>>
// Nevertheless, the following fields can be found in metadata.
////////////////////////////////////////////////////////////////////////////////
// 1. Array example
// {
//     "name": "split_offsets",
//     "type": [
//         "null",
//         {
//             "type": "array",
//             "items": "long"
//         }
//     ],
//     "default": null
// },
// 2. Map example
// {
//     "name": "upper_bounds",
//     "type": [
//         "null",
//         {
//             "type": "array",
//             "items": {
//                 "type": "record",
//                 "name": "k129_v130",
//                 "fields": [
//                     {
//                         "name": "key",
//                         "type": "int"
//                     },
//                     {
//                         "name": "value",
//                         "type": "bytes"
//                     }
//                 ]
//             }
//         }
//     ],
//     "default": null
// }
////////////////////////////////////////////////////////////////////////////////
// The spec probably implies that if an array contains nullable elements, those elements must be wrapped in a union.
// `split_offsets` and `upper_bounds` do not have optional elements, so there is no need to wrap them.

NodePtr MakeSchemaDataFile(const std::vector<PartitionKeyField>& partition_spec,
                           const ManifestEntryDeserializerConfig& cfg = {}) {
  auto result = std::make_shared<iceavro::RecordNode>("r2");
  result->AddField("content", std::make_shared<iceavro::IntNode>());
  result->AddField("file_path", std::make_shared<iceavro::StringNode>());
  result->AddField("file_format", std::make_shared<iceavro::StringNode>());
  result->AddField("record_count", std::make_shared<iceavro::LongNode>());
  result->AddField("file_size_in_bytes", std::make_shared<iceavro::LongNode>());
  result->AddField("equality_ids", std::make_shared<iceavro::OptionalNode>(
                                       std::make_shared<iceavro::ArrayNode>(std::make_shared<iceavro::IntNode>())));
  result->AddField("referenced_data_file",
                   std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::StringNode>()));
  if (cfg.datafile_config.extract_column_sizes) {
    auto column_sizes_entry = std::make_shared<iceavro::RecordNode>("k117_v118");
    column_sizes_entry->AddField("key", std::make_shared<iceavro::IntNode>());
    column_sizes_entry->AddField("value", std::make_shared<iceavro::LongNode>());
    result->AddField("column_sizes",
                     std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::ArrayNode>(column_sizes_entry)));
  }
  if (cfg.datafile_config.extract_value_counts) {
    auto value_counts_entry = std::make_shared<iceavro::RecordNode>("k119_v120");
    value_counts_entry->AddField("key", std::make_shared<iceavro::IntNode>());
    value_counts_entry->AddField("value", std::make_shared<iceavro::LongNode>());
    result->AddField("value_counts",
                     std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::ArrayNode>(value_counts_entry)));
  }
  if (cfg.datafile_config.extract_null_value_counts) {
    auto null_value_counts_entry = std::make_shared<iceavro::RecordNode>("k121_v122");
    null_value_counts_entry->AddField("key", std::make_shared<iceavro::IntNode>());
    null_value_counts_entry->AddField("value", std::make_shared<iceavro::LongNode>());
    result->AddField("null_value_counts", std::make_shared<iceavro::OptionalNode>(
                                              std::make_shared<iceavro::ArrayNode>(null_value_counts_entry)));
  }
  if (cfg.datafile_config.extract_lower_bounds) {
    auto lower_bounds_entry = std::make_shared<iceavro::RecordNode>("k126_v127");
    lower_bounds_entry->AddField("key", std::make_shared<iceavro::IntNode>());
    lower_bounds_entry->AddField("value", std::make_shared<iceavro::BytesNode>());
    result->AddField("lower_bounds",
                     std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::ArrayNode>(lower_bounds_entry)));
  }
  if (cfg.datafile_config.extract_upper_bounds) {
    auto upper_bounds_entry = std::make_shared<iceavro::RecordNode>("k129_v130");
    upper_bounds_entry->AddField("key", std::make_shared<iceavro::IntNode>());
    upper_bounds_entry->AddField("value", std::make_shared<iceavro::BytesNode>());
    result->AddField("upper_bounds",
                     std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::ArrayNode>(upper_bounds_entry)));
  }
  if (cfg.datafile_config.extract_nan_value_counts) {
    auto nan_value_counts_entry = std::make_shared<iceavro::RecordNode>("k138_v139");
    nan_value_counts_entry->AddField("key", std::make_shared<iceavro::IntNode>());
    nan_value_counts_entry->AddField("value", std::make_shared<iceavro::LongNode>());
    result->AddField("nan_value_counts", std::make_shared<iceavro::OptionalNode>(
                                             std::make_shared<iceavro::ArrayNode>(nan_value_counts_entry)));
  }
  if (cfg.datafile_config.extract_distinct_counts) {
    auto distinct_counts_entry = std::make_shared<iceavro::RecordNode>("k123_v124");
    distinct_counts_entry->AddField("key", std::make_shared<iceavro::IntNode>());
    distinct_counts_entry->AddField("value", std::make_shared<iceavro::LongNode>());
    result->AddField("distinct_counts", std::make_shared<iceavro::OptionalNode>(
                                            std::make_shared<iceavro::ArrayNode>(distinct_counts_entry)));
  }
  result->AddField("split_offsets", std::make_shared<iceavro::OptionalNode>(
                                        std::make_shared<iceavro::ArrayNode>(std::make_shared<iceavro::LongNode>())));
  result->AddField("sort_order_id", std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::IntNode>()));
  result->AddField("partition", MakeSchemaPartition(partition_spec));
  return result;
}

NodePtr MakeSchemaManifestEntry(const std::vector<PartitionKeyField>& partition_spec,
                                const ManifestEntryDeserializerConfig& cfg = {}) {
  auto result = std::make_shared<iceavro::RecordNode>("manifest_entry");

  result->AddField("status", std::make_shared<iceavro::IntNode>(0));
  result->AddField("snapshot_id", std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::LongNode>(1)));
  result->AddField("sequence_number", std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::LongNode>(3)));
  result->AddField("file_sequence_number",
                   std::make_shared<iceavro::OptionalNode>(std::make_shared<iceavro::LongNode>(4)));
  result->AddField("data_file", MakeSchemaDataFile(partition_spec, cfg));

  return result;
}

avro::GenericDatum& GetMember(avro::GenericRecord& datum, const std::string& key) {
  return datum.fieldAt(datum.fieldIndex(std::string(key)));
}

void SerializeBool(bool value, avro::GenericDatum& result) { result = avro::GenericDatum(value); }
void SerializeInt(int value, avro::GenericDatum& result) { result = avro::GenericDatum(value); }
void SerializeLong(int64_t value, avro::GenericDatum& result) { result = avro::GenericDatum(value); }
void SerializeFloat(float value, avro::GenericDatum& result) { result = avro::GenericDatum(value); }
void SerializeDouble(double value, avro::GenericDatum& result) { result = avro::GenericDatum(value); }
void SerializeString(std::string value, avro::GenericDatum& result) { result = avro::GenericDatum(std::move(value)); }
void SerializeBytes(std::vector<uint8_t> value, avro::GenericDatum& result) {
  result = avro::GenericDatum(std::move(value));
}
void SerializeFixed(const avro::ValidSchema& result_schema, DataFile::PartitionKey::Fixed value,
                    avro::GenericDatum& result) {
  auto fixed_value = avro::GenericFixed(result_schema.root(), std::move(value.bytes));
  result = avro::GenericDatum(fixed_value.schema(), std::move(fixed_value));
}

avro::GenericDatum SerializeOptionalWithValue(const avro::ValidSchema& result_schema, avro::GenericDatum&& datum) {
  avro::GenericUnion result(result_schema.root());
  result.selectBranch(1);
  result.datum() = std::move(datum);
  return avro::GenericDatum(result.schema(), std::move(result));
}

avro::GenericDatum SerializeOptionalWithoutValue(const avro::ValidSchema& result_schema) {
  avro::GenericUnion result(result_schema.root());
  result.selectBranch(0);
  result.datum() = avro::GenericDatum();
  return avro::GenericDatum(result.schema(), std::move(result));
}

void SerializeOptionalBool(std::optional<bool> value, avro::GenericDatum& result) {
  if (value.has_value()) {
    result.selectBranch(1);
    result.value<bool>() = *value;
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalInt(std::optional<int> value, avro::GenericDatum& result) {
  if (value.has_value()) {
    result.selectBranch(1);
    result.value<int32_t>() = *value;
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalLong(std::optional<int64_t> value, avro::GenericDatum& result) {
  if (value.has_value()) {
    result.selectBranch(1);
    result.value<int64_t>() = *value;
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalFloat(std::optional<float> value, avro::GenericDatum& result) {
  if (value.has_value()) {
    result.selectBranch(1);
    result.value<float>() = *value;
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalDouble(std::optional<double> value, avro::GenericDatum& result) {
  if (value.has_value()) {
    result.selectBranch(1);
    result.value<double>() = *value;
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalFixed(const std::optional<DataFile::PartitionKey::Fixed>& value, avro::GenericDatum& result) {
  if (value.has_value()) {
    result.selectBranch(1);
    auto& fixed_result = result.value<avro::GenericFixed>();
    fixed_result.value() = std::vector<uint8_t>(value->bytes);
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalString(const std::optional<std::string>& value, avro::GenericDatum& result) {
  if (value.has_value()) {
    result.selectBranch(1);
    result.value<std::string>() = *value;
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalBytes(const std::optional<std::vector<uint8_t>>& value, avro::GenericDatum& result) {
  if (value.has_value()) {
    result.selectBranch(1);
    result.value<std::vector<uint8_t>>() = *value;
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalVectorInt(const std::vector<int32_t>& value, avro::GenericDatum& result) {
  if (!value.empty()) {
    result.selectBranch(1);
    auto& array = result.value<avro::GenericArray>();
    for (const auto& x : value) {
      array.value().emplace_back(x);
    }
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalVectorLong(const std::vector<int64_t>& value, avro::GenericDatum& result) {
  if (!value.empty()) {
    result.selectBranch(1);
    auto& array = result.value<avro::GenericArray>();
    for (const auto& x : value) {
      array.value().emplace_back(x);
    }
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalMapIntLong(const std::map<int32_t, int64_t>& map, avro::GenericDatum& result) {
  if (!map.empty()) {
    result.selectBranch(1);
    auto& array = result.value<avro::GenericArray>();
    auto element_schema = array.schema()->leafAt(0);
    for (const auto& [k, v] : map) {
      avro::GenericDatum datum(element_schema);
      auto& record = datum.value<avro::GenericRecord>();
      SerializeInt(k, GetMember(record, "key"));
      SerializeLong(v, GetMember(record, "value"));
      array.value().emplace_back(std::move(datum));
    }
  } else {
    result.selectBranch(0);
  }
}

void SerializeOptionalMapIntBytes(const std::map<int32_t, std::vector<uint8_t>>& map, avro::GenericDatum& result) {
  if (!map.empty()) {
    result.selectBranch(1);
    auto& array = result.value<avro::GenericArray>();
    auto element_schema = array.schema()->leafAt(0);
    for (const auto& [k, v] : map) {
      avro::GenericDatum datum(element_schema);
      auto& record = datum.value<avro::GenericRecord>();
      SerializeInt(k, GetMember(record, "key"));
      SerializeBytes(v, GetMember(record, "value"));
      array.value().emplace_back(std::move(datum));
    }
  } else {
    result.selectBranch(0);
  }
}

static std::shared_ptr<const types::Type> ExtractTypeFromPartitionSpec(
    const std::vector<PartitionKeyField>& partition_spec, const std::string& name) {
  auto it = std::find_if(partition_spec.begin(), partition_spec.end(),
                         [&name](const auto& elem) { return elem.name == name; });
  if (it == partition_spec.end()) {
    return nullptr;
  }
  return it->type;
}

void SerializePartition(const std::vector<PartitionKeyField>& partition_spec, const DataFile::PartitionTuple& partition,
                        avro::GenericDatum& result) {
  avro::GenericRecord& record = result.value<avro::GenericRecord>();
  for (const auto& info : partition.fields) {
    if (record.hasField(info.name)) {
      std::shared_ptr<const types::Type> type = ExtractTypeFromPartitionSpec(partition_spec, info.name);
      Ensure(type != nullptr, std::string(__PRETTY_FUNCTION__) + ": internal error. Field '" + info.name +
                                  "' not found in partition_spec");
      bool is_null = std::holds_alternative<std::monostate>(info.value);
      switch (type->TypeId()) {
        case TypeID::kBoolean:
          SerializeOptionalBool(is_null ? std::nullopt : std::optional(std::get<bool>(info.value)),
                                GetMember(record, info.name));
          break;
        case TypeID::kInt:
        case TypeID::kDate:
          SerializeOptionalInt(is_null ? std::nullopt : std::optional(std::get<int>(info.value)),
                               GetMember(record, info.name));
          break;
        case TypeID::kFloat:
          SerializeOptionalFloat(is_null ? std::nullopt : std::optional(std::get<float>(info.value)),
                                 GetMember(record, info.name));
          break;
        case TypeID::kDouble:
          SerializeOptionalDouble(is_null ? std::nullopt : std::optional(std::get<double>(info.value)),
                                  GetMember(record, info.name));
          break;
        case TypeID::kBinary:
          SerializeOptionalBytes(is_null ? std::nullopt : std::optional(std::get<std::vector<uint8_t>>(info.value)),
                                 GetMember(record, info.name));
          break;
        case TypeID::kString:
          SerializeOptionalString(is_null ? std::nullopt : std::optional(std::get<std::string>(info.value)),
                                  GetMember(record, info.name));
          break;
        case TypeID::kLong:
        case TypeID::kTime:
        case TypeID::kTimestamp:
        case TypeID::kTimestamptz:
        case TypeID::kTimestampNs:
        case TypeID::kTimestamptzNs:
          SerializeOptionalLong(is_null ? std::nullopt : std::optional(std::get<int64_t>(info.value)),
                                GetMember(record, info.name));
          break;
        case TypeID::kDecimal:
        case TypeID::kFixed:
        case TypeID::kUuid:
          SerializeOptionalFixed(
              is_null ? std::nullopt : std::optional(std::get<DataFile::PartitionKey::Fixed>(info.value)),
              GetMember(record, info.name));
          break;
        default:
          throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": internal error. Field '" + info.name +
                                   "' has unexpected type " + type->ToString());
      }
    }
  }
}

void SerializeDataFile(const std::vector<PartitionKeyField>& partition_spec, const DataFile& data_file,
                       avro::GenericDatum& result) {
  auto& record = result.value<avro::GenericRecord>();
  SerializeLong(data_file.record_count, GetMember(record, "record_count"));
  SerializeLong(data_file.file_size_in_bytes, GetMember(record, "file_size_in_bytes"));
  SerializeOptionalVectorLong(data_file.split_offsets, GetMember(record, "split_offsets"));
  SerializeOptionalVectorInt(data_file.equality_ids, GetMember(record, "equality_ids"));
  SerializeString(data_file.file_format, GetMember(record, "file_format"));
  SerializeString(data_file.file_path, GetMember(record, "file_path"));
  SerializeInt(static_cast<int>(data_file.content), GetMember(record, "content"));
  SerializeOptionalString(data_file.referenced_data_file, GetMember(record, "referenced_data_file"));
  SerializeOptionalInt(data_file.sort_order_id, GetMember(record, "sort_order_id"));
  SerializeOptionalMapIntLong(data_file.column_sizes, GetMember(record, "column_sizes"));
  SerializeOptionalMapIntLong(data_file.value_counts, GetMember(record, "value_counts"));
  SerializeOptionalMapIntLong(data_file.null_value_counts, GetMember(record, "null_value_counts"));
  SerializeOptionalMapIntBytes(data_file.lower_bounds, GetMember(record, "lower_bounds"));
  SerializeOptionalMapIntBytes(data_file.upper_bounds, GetMember(record, "upper_bounds"));
  SerializeOptionalMapIntLong(data_file.distinct_counts, GetMember(record, "distinct_counts"));
  SerializeOptionalMapIntLong(data_file.nan_value_counts, GetMember(record, "nan_value_counts"));
  SerializePartition(partition_spec, data_file.partition_tuple, GetMember(record, "partition"));
}

void SerializeManifestEntry(const std::vector<PartitionKeyField>& partition_spec, const ManifestEntry& entry,
                            avro::GenericDatum& result) {
  auto& record = result.value<avro::GenericRecord>();
  SerializeInt(static_cast<int>(entry.status), GetMember(record, "status"));
  SerializeOptionalLong(entry.snapshot_id, GetMember(record, "snapshot_id"));
  SerializeOptionalLong(entry.sequence_number, GetMember(record, "sequence_number"));
  SerializeOptionalLong(entry.file_sequence_number, GetMember(record, "file_sequence_number"));
  SerializeDataFile(partition_spec, entry.data_file, GetMember(record, "data_file"));
}
}  // namespace

avro::ValidSchema AvroSchemaFromNode(NodePtr node) {
  return avro::compileJsonSchemaFromString(iceavro::ToJsonString(node));
}

avro::NodePtr Find(avro::NodePtr root, const std::string& name) {
  size_t leaves = root->leaves();
  for (size_t i = 0; i < leaves; ++i) {
    if (root->nameAt(i) == name) {
      return root->leafAt(i);
    }
  }

  return nullptr;
}

void ValidatePartitionSpec(const std::vector<PartitionKeyField>& partition_spec, avro::NodePtr root) {
  avro::NodePtr data_file = Find(root, "data_file");
  Ensure(data_file != nullptr, std::string(__PRETTY_FUNCTION__) + ": data_file is not found in avro.schema");

  avro::NodePtr partition = Find(data_file, "partition");
  Ensure(partition != nullptr, std::string(__PRETTY_FUNCTION__) + ": partition is not found in avro.schema");

  Ensure(partition_spec.size() == partition->leaves(),
         std::string(__PRETTY_FUNCTION__) + ": partition_spec has " + std::to_string(partition_spec.size()) +
             ", but partition in avro contains " + std::to_string(partition->leaves()) + " fields");

  // TODO(gmusya): improve test coverage
  std::map<std::string, int> name_to_leaf;
  for (int i = 0; i < partition->names(); ++i) {
    name_to_leaf[partition->nameAt(i)] = i;
  }
  for (const auto& spec_field : partition_spec) {
    Ensure(name_to_leaf.contains(spec_field.name), std::string(__PRETTY_FUNCTION__) + ": partition_spec has field '" +
                                                       spec_field.name + "', but partition in avro does not");

    // TODO(gmusya): validate logical types
  }
}

class ManifestEntryStream : public IcebergEntriesStream {
 public:
  ManifestEntryStream(std::string content, const std::vector<PartitionKeyField>& partition_spec,
                      const ManifestEntryDeserializerConfig& config, bool use_reader_schema)
      : input_(std::move(content)),
        data_file_reader_([&]() {
          Ensure(!!input_, std::string(__PRETTY_FUNCTION__) + ": input is invalid");

          auto istream = avro::istreamInputStream(input_);
          if (use_reader_schema) {
            auto schema = AvroSchemaFromNode(MakeSchemaManifestEntry(partition_spec, config));
            return avro::DataFileReader<avro::GenericDatum>(std::move(istream), schema);
          } else {
            return avro::DataFileReader<avro::GenericDatum>(std::move(istream));
          }
        }()),
        deserializer_(config) {
    ValidatePartitionSpec(partition_spec, data_file_reader_.dataSchema().root());
  }

  std::optional<ManifestEntry> ReadNext() override {
    while (true) {
      avro::GenericDatum manifest_entry(data_file_reader_.readerSchema());
      if (data_file_reader_.read(manifest_entry)) {
        ManifestEntry entry = deserializer_.Deserialize(manifest_entry);
        return entry;
      }

      return std::nullopt;
    }
  }

 private:
  std::stringstream input_;
  avro::DataFileReader<avro::GenericDatum> data_file_reader_;
  ManifestEntryDeserializer deserializer_;
};

std::shared_ptr<IcebergEntriesStream> MakeManifestEntriesStream(std::string data,
                                                                const std::vector<PartitionKeyField>& partition_spec,
                                                                const ManifestEntryDeserializerConfig& config,
                                                                bool use_reader_schema) {
  return std::make_shared<ManifestEntryStream>(std::move(data), partition_spec, config, use_reader_schema);
}

Manifest ReadManifestEntries(std::istream& input, const std::vector<PartitionKeyField>& partition_spec,
                             const ManifestEntryDeserializerConfig& config, bool use_reader_schema) {
  Ensure(!!input, std::string(__PRETTY_FUNCTION__) + ": input is invalid");

  auto istream = avro::istreamInputStream(input);
  avro::DataFileReader<avro::GenericDatum> data_file_reader = [&]() {
    if (use_reader_schema) {
      auto schema = AvroSchemaFromNode(MakeSchemaManifestEntry(partition_spec, config));
      return avro::DataFileReader<avro::GenericDatum>(std::move(istream), schema);
    } else {
      return avro::DataFileReader<avro::GenericDatum>(std::move(istream));
    }
  }();

  ValidatePartitionSpec(partition_spec, data_file_reader.dataSchema().root());

  Manifest result;

  result.metadata = data_file_reader.metadata();

  result.metadata.erase("avro.schema");
  result.metadata.erase("avro.codec");

  if (!result.metadata.contains("schema-id") && result.metadata.contains("schema")) {
    rapidjson::Document document;
    const auto& schema = result.metadata["schema"];
    std::string schema_str(schema.begin(), schema.end());
    document.Parse(schema_str.c_str());
    if (document.IsObject()) {
      std::string schema_id_str = std::to_string(document["schema-id"].GetInt());
      std::vector<uint8_t> schema_id_bytes(schema_id_str.begin(), schema_id_str.end());
      result.metadata["schema-id"] = schema_id_bytes;
    }
  }
  ManifestEntryDeserializer deserializer(config);
  while (true) {
    avro::GenericDatum manifest_entry(data_file_reader.readerSchema());
    if (data_file_reader.read(manifest_entry)) {
      ManifestEntry entry = deserializer.Deserialize(manifest_entry);
      result.entries.emplace_back(std::move(entry));
    } else {
      break;
    }
  }
  return result;
}

Manifest ReadManifestEntries(const std::string& data, const std::vector<PartitionKeyField>& partition_spec,
                             const ManifestEntryDeserializerConfig& config, bool use_reader_schema) {
  std::stringstream ss(data);
  return ReadManifestEntries(ss, partition_spec, config, use_reader_schema);
}

std::string WriteManifestEntries(const Manifest& manifest, const std::vector<PartitionKeyField>& partition_spec) {
  static constexpr size_t bufferSize = 1024 * 1024;

  std::stringstream ss;
  auto ostream = avro::ostreamOutputStream(ss, bufferSize);

  auto schema = AvroSchemaFromNode(MakeSchemaManifestEntry(partition_spec));

  avro::DataFileWriter<avro::GenericDatum> writer(std::move(ostream), schema, 16 * 1024, avro::NULL_CODEC,
                                                  manifest.metadata);

  for (auto& man_entry : manifest.entries) {
    avro::GenericDatum datum(schema);
    SerializeManifestEntry(partition_spec, man_entry, datum);
    writer.write(datum);
  }
  writer.close();

  return ss.str();
}

void FillManifestSplitOffsets(std::vector<ManifestEntry>& data, std::shared_ptr<arrow::fs::FileSystem> fs) {
  for (size_t i = 0; i < data.size(); ++i) {
    uint64_t file_size = 0;
    auto parquet_meta = ParquetMetadata(fs, data[i].data_file.file_path, file_size);
    data[i].data_file.split_offsets = SplitOffsets(parquet_meta);
  }
}

void FillManifestSplitOffsets(std::vector<ManifestEntry>& data,
                              const std::vector<std::shared_ptr<parquet::FileMetaData>>& metadata) {
  for (size_t i = 0; i < data.size(); ++i) {
    uint64_t file_size = 0;
    data[i].data_file.split_offsets = SplitOffsets(metadata[i]);
  }
}

}  // namespace ice_tea

}  // namespace iceberg
