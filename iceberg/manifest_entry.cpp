#include "iceberg/manifest_entry.h"

#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <type_traits>

#include "parquet/arrow/reader.h"
#include "parquet/metadata.h"
#include "parquet/statistics.h"

// Unfortunately, there is no adequate way to extract logical type from GenericDatum other than this
// clang-format off
#define protected public
#include "avro/GenericDatum.hh"
// clang-format on

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Generic.hh"
#include "avro/Schema.hh"
#include "avro/Types.hh"
#include "avro/ValidSchema.hh"
#include "iceberg/type.h"
#include "rapidjson/document.h"

namespace {
inline avro::LogicalType GetLogicalTypeFromDatum(const avro::GenericDatum& datum) {
  return (datum.type_ == avro::AVRO_UNION) ?
#if __cplusplus >= 201703L
                                           std::any_cast<avro::GenericUnion>(&datum.value_)->datum().logicalType()
                                           :
#else
                                           boost::any_cast<avro::GenericUnion>(&datum.value_)->datum().logicalType()
                                           :
#endif
                                           datum.logicalType_;
}
}  // namespace

namespace iceberg {

std::vector<int64_t> SplitOffsets(std::shared_ptr<parquet::FileMetaData> parquet_meta) {
  std::vector<int64_t> split_offsets;
  split_offsets.reserve(parquet_meta->num_row_groups());
  for (int i = 0; i < parquet_meta->num_row_groups(); ++i) {
    auto rg_meta = parquet_meta->RowGroup(i);
    if (!rg_meta) {
      throw std::runtime_error("No row group for id " + std::to_string(i));
    }
    split_offsets.push_back(rg_meta->file_offset());
  }
  std::sort(split_offsets.begin(), split_offsets.end());
  return split_offsets;
}

std::shared_ptr<parquet::FileMetaData> ParquetMetadata(std::shared_ptr<arrow::io::RandomAccessFile> input_file) {
  parquet::arrow::FileReaderBuilder reader_builder;
  auto status = reader_builder.Open(input_file, parquet::default_reader_properties());
  if (!status.ok()) {
    throw std::runtime_error("cannot open parquet file");
  }

  reader_builder.memory_pool(arrow::default_memory_pool());
  auto maybe_arrow_reader = reader_builder.Build();
  if (!maybe_arrow_reader.ok()) {
    throw maybe_arrow_reader.status();
  }
  auto arrow_reader = maybe_arrow_reader.MoveValueUnsafe();
  return arrow_reader->parquet_reader()->metadata();
}

std::shared_ptr<parquet::FileMetaData> ParquetMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                       const std::string& file_path, uint64_t& file_size) {
  auto input_file = fs->OpenInputFile(file_path);
  if (!input_file.ok()) {
    throw std::runtime_error("Cannot open input file: " + file_path);
  }
  auto size_rez = (*input_file)->GetSize();
  if (!size_rez.ok()) {
    throw std::runtime_error("Cannot get input file size: " + file_path);
  }
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

template <typename T>
T Extract(const avro::GenericDatum& datum, const std::string& name)
requires(std::is_same_v<std::vector<typename T::value_type>, T> ||
         std::is_same_v<std::optional<typename T::value_type>, T> ||
         std::is_same_v<std::map<typename T::key_type, typename T::value_type::second_type>, T>);
// clang-format on

template <typename T>
T Extract(const avro::GenericRecord& datum, const std::string& name) {
  if (!datum.hasField(name)) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": field '" + name + "' is missing");
  }
  const auto& field = datum.field(name);
  return Deserialize<T>(field);
}

// clang-format off
template <typename T>
T Extract(const avro::GenericRecord& datum, const std::string& name)
requires(std::is_same_v<std::vector<typename T::value_type>, T> ||
         std::is_same_v<std::optional<typename T::value_type>, T> ||
         std::is_same_v<std::map<typename T::key_type, typename T::value_type::second_type>, T>)
{
  if (!datum.hasField(name)) {
    return T{};
  }
  const auto& field = datum.field(name);
  return Deserialize<T>(field);
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
  if (datum.type() != avro::AVRO_RECORD) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");
  }
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
  if (datum.type() != avro::AVRO_INT) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");
  }
  return datum.value<int>();
}

template <>
int64_t Deserialize(const avro::GenericDatum& datum) {
  if (datum.type() != avro::AVRO_LONG) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");
  }
  return datum.value<int64_t>();
}

template <>
std::string Deserialize(const avro::GenericDatum& datum) {
  if (datum.type() != avro::AVRO_STRING) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");
  }
  return datum.value<std::string>();
}

template <>
DataFile::PartitionTuple Deserialize(const avro::GenericDatum& datum) {
  if (datum.type() != avro::AVRO_RECORD) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");
  }
  const auto& record = datum.value<avro::GenericRecord>();
  DataFile::PartitionTuple result;
  const auto& schema = record.schema();
  for (size_t i = 0; i < schema->names(); ++i) {
    auto name = schema->nameAt(i);

    const auto& field = record.fieldAt(record.fieldIndex(name));
    const auto logical_type = GetLogicalTypeFromDatum(field);

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
      } else {
        result.fields.emplace_back(name, std::move(fixed), std::make_shared<types::FixedType>(fixed.bytes.size()));
      }

      continue;
    }

    if (logical_type.type() != avro::LogicalType::NONE) {
      throw std::runtime_error("Unexpected logical type in avro: " +
                               std::to_string(static_cast<int>(logical_type.type())));
    }

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

template <>
DataFile Deserialize(const avro::GenericDatum& datum) {
  if (datum.type() != avro::AVRO_RECORD) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": data.type() is not record");
  }
  const auto& record = datum.value<avro::GenericRecord>();

  DataFile data_file;
  data_file.file_path = Extract<std::string>(record, "file_path");
  data_file.content = static_cast<DataFile::FileContent>(Extract<int>(record, "content"));
  data_file.file_format = Extract<std::string>(record, "file_format");
  data_file.record_count = Extract<int64_t>(record, "record_count");
  data_file.file_size_in_bytes = Extract<int64_t>(record, "file_size_in_bytes");
  data_file.sort_order_id = Extract<std::optional<int32_t>>(record, "sort_order_id");
  data_file.referenced_data_file = Extract<std::optional<std::string>>(record, "referenced_data_file");
  data_file.column_sizes = Extract<std::map<int32_t, int64_t>>(record, "column_sizes");
  data_file.value_counts = Extract<std::map<int32_t, int64_t>>(record, "value_counts");
  data_file.split_offsets = Extract<std::vector<int64_t>>(record, "split_offsets");
  data_file.null_value_counts = Extract<std::map<int32_t, int64_t>>(record, "null_value_counts");
  data_file.lower_bounds = Extract<std::map<int32_t, std::vector<uint8_t>>>(record, "lower_bounds");
  data_file.upper_bounds = Extract<std::map<int32_t, std::vector<uint8_t>>>(record, "upper_bounds");
  data_file.partition_tuple = Extract<DataFile::PartitionTuple>(record, "partition");
  data_file.equality_ids = Extract<std::vector<int32_t>>(record, "equality_ids");
  return data_file;
}

template <>
ManifestEntry Deserialize(const avro::GenericDatum& datum) {
  if (datum.type() != avro::AVRO_RECORD) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": datum.type() is not record");
  }
  const auto& record = datum.value<avro::GenericRecord>();

  ManifestEntry entry;
  entry.status = static_cast<ManifestEntry::Status>(Extract<int>(record, "status"));
  entry.snapshot_id = Extract<std::optional<int64_t>>(record, "snapshot_id");
  entry.sequence_number = Extract<std::optional<int64_t>>(record, "sequence_number");
  entry.file_sequence_number = Extract<std::optional<int64_t>>(record, "file_sequence_number");
  entry.data_file = Extract<DataFile>(record, "data_file");
  return entry;
}

void AddField(avro::RecordSchema& result_schema, const std::string& field_name, const avro::ValidSchema& field_schema,
              const std::optional<std::string>& doc = std::nullopt) {
  if (doc.has_value()) {
    field_schema.root()->setDoc(*doc);
  }
  result_schema.root()->addName(field_name);
  result_schema.root()->addLeaf(field_schema.root());
}

avro::ValidSchema MakeSchemaOptional(const avro::ValidSchema& schema) {
  avro::UnionSchema result;
  result.addType(avro::NullSchema());
  result.root()->addLeaf(schema.root());
  return avro::ValidSchema(result);
}

avro::ValidSchema MakeSchemaPair(const std::string& name, const avro::ValidSchema& key,
                                 const avro::ValidSchema& other) {
  avro::RecordSchema schema(name);
  AddField(schema, "key", key);
  AddField(schema, "value", other);
  return avro::ValidSchema(schema);
}

avro::ValidSchema MakeSchemaArray(const avro::ValidSchema& element) {
  avro::NodePtr arr(new avro::NodeArray);
  arr->addLeaf(element.root());
  return avro::ValidSchema(arr);
}

avro::ValidSchema MakeSchemaMap(const std::string& element_name, const avro::ValidSchema& key,
                                const avro::ValidSchema& other) {
  return MakeSchemaArray(MakeSchemaPair(element_name, key, other));
}

avro::ValidSchema MakeSchemaBool() { return avro::ValidSchema(avro::BoolSchema()); }
avro::ValidSchema MakeSchemaInt() { return avro::ValidSchema(avro::IntSchema()); }
avro::ValidSchema MakeSchemaDate() {
  auto schema = avro::IntSchema();
  schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::DATE));
  return avro::ValidSchema(schema);
}
avro::ValidSchema MakeSchemaTime() {
  auto schema = avro::LongSchema();
  schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::TIME_MICROS));
  return avro::ValidSchema(schema);
}
avro::ValidSchema MakeSchemaTimestamp() {
  auto schema = avro::LongSchema();
  schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS));
  return avro::ValidSchema(schema);
}
avro::ValidSchema MakeSchemaTimestamptz() {
  return MakeSchemaTimestamp();  // timestamp and timestamptz are same types for avro
}
constexpr int MinimumBytesToStoreDecimalUnsafe(int precision) {
  int bytes = 1;
  __uint128_t minimum_nonrepresentable_value =
      (1 << 7);  // not (1 << 8) because both positive and negative values must be representable

  const __uint128_t value_to_store = [precision]() {
    __uint128_t result = 1;
    for (int i = 0; i < precision; ++i) {
      result *= 10;
    }
    return result;
  }();

  while (bytes < 16 && minimum_nonrepresentable_value <= value_to_store) {
    ++bytes;
    minimum_nonrepresentable_value *= (1 << 8);
  }
  return bytes;
}
static_assert(MinimumBytesToStoreDecimalUnsafe(1) == 1);
static_assert(MinimumBytesToStoreDecimalUnsafe(9) == 4);
static_assert(MinimumBytesToStoreDecimalUnsafe(10) == 5);
static_assert(MinimumBytesToStoreDecimalUnsafe(18) == 8);
static_assert(MinimumBytesToStoreDecimalUnsafe(19) == 9);
static_assert(MinimumBytesToStoreDecimalUnsafe(38) == 16);

constexpr int MinimumBytesToStoreDecimal(int precision) {
  const int kMaxPrecision = 38;
  if (precision > kMaxPrecision) {
    throw std::runtime_error("MinimumBytesToStoreDecimal: precision is greater than " + std::to_string(kMaxPrecision) +
                             " is not supported");
  }
  if (precision <= 0) {
    throw std::runtime_error("MinimumBytesToStoreDecimal: precision is less than or equal to " + std::to_string(0) +
                             " is not supported");
  }
  return MinimumBytesToStoreDecimalUnsafe(precision);
}

avro::ValidSchema MakeSchemaDecimal(int precision, int scale) {
  auto schema = avro::FixedSchema(MinimumBytesToStoreDecimal(precision),
                                  "decimal_" + std::to_string(precision) + "_" + std::to_string(scale));
  avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
  logical_type.setPrecision(precision);
  logical_type.setScale(scale);
  schema.root()->setLogicalType(logical_type);
  return avro::ValidSchema(schema);
}
avro::ValidSchema MakeSchemaFixed(int size) { return avro::ValidSchema(avro::FixedSchema(size, "fixed")); }
avro::ValidSchema MakeSchemaUuid() {
  auto schema = avro::FixedSchema(16, "uuid_fixed");
// UUID is serialized without logical type
#if 0
  schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::UUID));
#endif
  return avro::ValidSchema(schema);
}
avro::ValidSchema MakeSchemaLong() { return avro::ValidSchema(avro::LongSchema()); }
avro::ValidSchema MakeSchemaFloat() { return avro::ValidSchema(avro::FloatSchema()); }
avro::ValidSchema MakeSchemaDouble() { return avro::ValidSchema(avro::DoubleSchema()); }
avro::ValidSchema MakeSchemaString() { return avro::ValidSchema(avro::StringSchema()); }
avro::ValidSchema MakeSchemaBytes() { return avro::ValidSchema(avro::BytesSchema()); }
avro::ValidSchema MakeSchemaFixed(int size, const std::string& name) {
  return avro::ValidSchema(avro::FixedSchema(size, name));
}

avro::ValidSchema MakeSchemaPartition(const std::vector<PartitionKeyField>& partition_spec) {
  avro::RecordSchema schema("r102");
  for (const auto& field : partition_spec) {
    switch (field.type->TypeId()) {
      case TypeID::kBoolean:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaBool()));
        break;
      case TypeID::kInt:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaInt()));
        break;
      case TypeID::kLong:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaLong()));
        break;
      case TypeID::kDate:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaDate()));
        break;
      case TypeID::kTime:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaTime()));
        break;
      // timestamp and timestamptz are same types for avro
      case TypeID::kTimestamptz:
      case TypeID::kTimestamp:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaTimestamp()));
        break;
      case TypeID::kDecimal: {
        auto decimal_type = std::static_pointer_cast<const types::DecimalType>(field.type);
        AddField(schema, field.name,
                 MakeSchemaOptional(MakeSchemaDecimal(decimal_type->Precision(), decimal_type->Scale())));
        break;
      }
      case TypeID::kFixed: {
        auto fixed_type = std::static_pointer_cast<const types::FixedType>(field.type);
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaFixed(fixed_type->Size())));
        break;
      }
      case TypeID::kUuid:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaUuid()));
        break;
      case TypeID::kString:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaString()));
        break;
      case TypeID::kBinary:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaBytes()));
        break;
      case TypeID::kFloat:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaFloat()));
        break;
      case TypeID::kDouble:
        AddField(schema, field.name, MakeSchemaOptional(MakeSchemaDouble()));
        break;
      default:
        throw std::runtime_error("Unexpected data iceberg::TypeID in partitioning: " +
                                 std::to_string(static_cast<int>(field.type->TypeId())));
    }
  }
  return avro::ValidSchema(schema);
}

avro::ValidSchema MakeSchemaDataFile(const std::vector<PartitionKeyField>& partition_spec) {
  avro::RecordSchema schema("r2");
  AddField(schema, "file_path", MakeSchemaString());
  AddField(schema, "file_format", MakeSchemaString());
  AddField(schema, "content", MakeSchemaInt());
  AddField(schema, "file_size_in_bytes", MakeSchemaLong());
  AddField(schema, "record_count", MakeSchemaLong());
  AddField(schema, "split_offsets", MakeSchemaOptional(MakeSchemaArray(MakeSchemaLong())));
  AddField(schema, "equality_ids", MakeSchemaOptional(MakeSchemaArray(MakeSchemaInt())));
  AddField(schema, "sort_order_id", MakeSchemaOptional(MakeSchemaInt()));
  AddField(schema, "referenced_data_file", MakeSchemaOptional(MakeSchemaString()));
  AddField(schema, "column_sizes", MakeSchemaOptional(MakeSchemaMap("k117_v118", MakeSchemaInt(), MakeSchemaLong())));
  AddField(schema, "value_counts", MakeSchemaOptional(MakeSchemaMap("k119_v120", MakeSchemaInt(), MakeSchemaLong())));
  AddField(schema, "null_value_counts",
           MakeSchemaOptional(MakeSchemaMap("k121_v122", MakeSchemaInt(), MakeSchemaLong())));
  AddField(schema, "lower_bounds", MakeSchemaOptional(MakeSchemaMap("k126_v127", MakeSchemaInt(), MakeSchemaBytes())));
  AddField(schema, "upper_bounds", MakeSchemaOptional(MakeSchemaMap("k129_v130", MakeSchemaInt(), MakeSchemaBytes())));
  AddField(schema, "partition", MakeSchemaPartition(partition_spec));
  return avro::ValidSchema(schema);
}

avro::ValidSchema MakeSchemaManifestEntry(const std::vector<PartitionKeyField>& partition_spec) {
  avro::RecordSchema schema("manifest_entry");
  AddField(schema, "status", MakeSchemaInt());
  AddField(schema, "snapshot_id", MakeSchemaOptional(MakeSchemaLong()));
  AddField(schema, "sequence_number", MakeSchemaOptional(MakeSchemaLong()));
  AddField(schema, "file_sequence_number", MakeSchemaOptional(MakeSchemaLong()));
  AddField(schema, "data_file", MakeSchemaDataFile(partition_spec));

  return avro::ValidSchema(schema);
}

void AddMember(avro::GenericRecord& result, const std::string& key, avro::GenericDatum&& value) {
  result.setFieldAt(result.fieldIndex(std::string(key)), value);
}

avro::GenericDatum SerializeBool(bool value) { return avro::GenericDatum(value); }
avro::GenericDatum SerializeInt(int value) { return avro::GenericDatum(value); }
avro::GenericDatum SerializeLong(int64_t value) { return avro::GenericDatum(value); }
avro::GenericDatum SerializeFloat(float value) { return avro::GenericDatum(value); }
avro::GenericDatum SerializeDouble(double value) { return avro::GenericDatum(value); }
avro::GenericDatum SerializeString(std::string value) { return avro::GenericDatum(std::move(value)); }
avro::GenericDatum SerializeBytes(std::vector<uint8_t> value) { return avro::GenericDatum(std::move(value)); }
avro::GenericDatum SerializeFixed(const avro::ValidSchema& result_schema, DataFile::PartitionKey::Fixed value) {
  auto fixed_value = avro::GenericFixed(result_schema.root(), std::move(value.bytes));
  return avro::GenericDatum(fixed_value.schema(), std::move(fixed_value));
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

avro::GenericDatum SerializeOptionalInt(std::optional<int> value) {
  auto result_schema = MakeSchemaOptional(MakeSchemaInt());
  if (value.has_value()) {
    return SerializeOptionalWithValue(result_schema, SerializeInt(*value));
  } else {
    return SerializeOptionalWithoutValue(result_schema);
  }
}

avro::GenericDatum SerializeOptionalLong(std::optional<int64_t> value) {
  auto result_schema = MakeSchemaOptional(MakeSchemaLong());
  if (value.has_value()) {
    return SerializeOptionalWithValue(result_schema, SerializeLong(*value));
  } else {
    return SerializeOptionalWithoutValue(result_schema);
  }
}

avro::GenericDatum SerializeOptionalBool(std::optional<bool> value) {
  auto result_schema = MakeSchemaOptional(MakeSchemaBool());
  if (value.has_value()) {
    return SerializeOptionalWithValue(result_schema, SerializeBool(*value));
  } else {
    return SerializeOptionalWithoutValue(result_schema);
  }
}

avro::GenericDatum SerializeOptionalFloat(std::optional<float> value) {
  auto result_schema = MakeSchemaOptional(MakeSchemaFloat());
  if (value.has_value()) {
    return SerializeOptionalWithValue(result_schema, SerializeFloat(*value));
  } else {
    return SerializeOptionalWithoutValue(result_schema);
  }
}

avro::GenericDatum SerializeOptionalDouble(std::optional<double> value) {
  auto result_schema = MakeSchemaOptional(MakeSchemaDouble());
  if (value.has_value()) {
    return SerializeOptionalWithValue(result_schema, SerializeDouble(*value));
  } else {
    return SerializeOptionalWithoutValue(result_schema);
  }
}

avro::GenericDatum SerializeOptionalString(std::optional<std::string> value) {
  auto result_schema = MakeSchemaOptional(MakeSchemaString());
  if (value.has_value()) {
    return SerializeOptionalWithValue(result_schema, SerializeString(std::move(*value)));
  } else {
    return SerializeOptionalWithoutValue(result_schema);
  }
}

avro::GenericDatum SerializeOptionalBytes(std::optional<std::vector<uint8_t>> value) {
  auto result_schema = MakeSchemaOptional(MakeSchemaBytes());
  if (value.has_value()) {
    return SerializeOptionalWithValue(result_schema, SerializeBytes(std::move(*value)));
  } else {
    return SerializeOptionalWithoutValue(result_schema);
  }
}

avro::GenericDatum SerializeOptionalFixed(std::optional<DataFile::PartitionKey::Fixed> value) {
  auto fixed_schema = MakeSchemaFixed(value->bytes.size(), "fixed");
  auto result_schema = MakeSchemaOptional(fixed_schema);
  if (value.has_value()) {
    return SerializeOptionalWithValue(result_schema, SerializeFixed(fixed_schema, std::move(*value)));
  } else {
    return SerializeOptionalWithoutValue(result_schema);
  }
}

avro::GenericDatum SerializeNull() { return avro::GenericDatum(); }

avro::GenericDatum SerializePairIntLong(std::pair<int32_t, int64_t> pair, const std::string& element_name) {
  const auto& pair_schema = MakeSchemaPair(element_name, MakeSchemaInt(), MakeSchemaLong());
  avro::GenericRecord record(pair_schema.root());
  AddMember(record, "key", SerializeInt(pair.first));
  AddMember(record, "value", SerializeLong(pair.second));
  return avro::GenericDatum(record.schema(), record);
}

avro::GenericDatum SerializePairIntBytes(const std::pair<int32_t, std::vector<uint8_t>>& pair,
                                         const std::string& element_name) {
  const auto& pair_schema = MakeSchemaPair(element_name, MakeSchemaInt(), MakeSchemaLong());
  avro::GenericRecord record(pair_schema.root());
  AddMember(record, "key", SerializeInt(pair.first));
  AddMember(record, "value", SerializeBytes(pair.second));
  return avro::GenericDatum(record.schema(), record);
}

avro::GenericDatum SerializeVectorGenericDatum(const avro::ValidSchema& result_schema,
                                               std::vector<avro::GenericDatum>&& data) {
  avro::GenericArray array(result_schema.root());
  array.value() = std::move(data);
  return avro::GenericDatum(array.schema(), std::move(array));
}

avro::GenericDatum SerializeVectorLong(const std::vector<int64_t>& array) {
  std::vector<avro::GenericDatum> result;
  for (const int64_t& value : array) {
    result.emplace_back(SerializeLong(value));
  }
  auto result_schema = MakeSchemaArray(MakeSchemaLong());
  return SerializeVectorGenericDatum(result_schema, std::move(result));
}

avro::GenericDatum SerializeVectorInt(const std::vector<int>& array) {
  std::vector<avro::GenericDatum> result;
  for (const int32_t& value : array) {
    result.emplace_back(SerializeInt(value));
  }
  auto result_schema = MakeSchemaArray(MakeSchemaInt());
  return SerializeVectorGenericDatum(result_schema, std::move(result));
}

avro::GenericDatum SerializeMapIntLong(const std::map<int32_t, int64_t>& map, const std::string& element_name) {
  std::vector<avro::GenericDatum> array;
  for (const auto& p : map) {
    array.emplace_back(SerializePairIntLong(p, element_name));
  }
  auto result_schema = MakeSchemaMap(element_name, MakeSchemaInt(), MakeSchemaLong());
  return SerializeVectorGenericDatum(result_schema, std::move(array));
}

avro::GenericDatum SerializeMapIntBytes(const std::map<int32_t, std::vector<uint8_t>>& map,
                                        const std::string& element_name) {
  std::vector<avro::GenericDatum> array;
  for (const auto& p : map) {
    array.emplace_back(SerializePairIntBytes(p, element_name));
  }
  auto result_schema = MakeSchemaMap(element_name, MakeSchemaInt(), MakeSchemaBytes());
  return SerializeVectorGenericDatum(result_schema, std::move(array));
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

avro::GenericDatum SerializePartition(const std::vector<PartitionKeyField>& partition_spec,
                                      const DataFile::PartitionTuple& partition) {
  const auto partition_schema = MakeSchemaPartition(partition_spec);
  avro::GenericRecord record(partition_schema.root());
  for (const auto& info : partition.fields) {
    if (record.hasField(info.name)) {
      std::shared_ptr<const types::Type> type = ExtractTypeFromPartitionSpec(partition_spec, info.name);
      if (!type) {
        throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": internal error. Field '" + info.name +
                                 "' not found in partition_spec");
      }
      auto datum = [&]() {
        bool is_null = std::holds_alternative<std::monostate>(info.value);
        switch (type->TypeId()) {
          case TypeID::kBoolean:
            return SerializeOptionalBool(is_null ? std::nullopt : std::optional(std::get<bool>(info.value)));
          case TypeID::kInt:
          case TypeID::kDate:
            return SerializeOptionalInt(is_null ? std::nullopt : std::optional(std::get<int>(info.value)));
          case TypeID::kLong:
          case TypeID::kTime:
          case TypeID::kTimestamp:
          case TypeID::kTimestamptz:
            return SerializeOptionalLong(is_null ? std::nullopt : std::optional(std::get<int64_t>(info.value)));
          case TypeID::kFloat:
            return SerializeOptionalFloat(is_null ? std::nullopt : std::optional(std::get<float>(info.value)));
          case TypeID::kDouble:
            return SerializeOptionalDouble(is_null ? std::nullopt : std::optional(std::get<double>(info.value)));
          case TypeID::kString:
            return SerializeOptionalString(is_null ? std::nullopt : std::optional(std::get<std::string>(info.value)));
          case TypeID::kBinary:
            return SerializeOptionalBytes(is_null ? std::nullopt
                                                  : std::optional(std::get<std::vector<uint8_t>>(info.value)));
          case TypeID::kDecimal:
          case TypeID::kFixed:
          case TypeID::kUuid:
            return SerializeOptionalFixed(is_null ? std::nullopt
                                                  : std::optional(std::get<DataFile::PartitionKey::Fixed>(info.value)));
          default:
            throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": internal error. Field '" + info.name +
                                     "' has unexpected type " + type->ToString());
        }
      }();
      record.setFieldAt(record.fieldIndex(info.name), std::move(datum));
    }
  }

  return avro::GenericDatum(record.schema(), std::move(record));
}

avro::GenericDatum SerializeDataFile(const std::vector<PartitionKeyField>& partition_spec, const DataFile& data_file) {
  const auto data_schema = MakeSchemaDataFile(partition_spec);
  avro::GenericRecord result(data_schema.root());
  AddMember(result, "record_count", SerializeLong(data_file.record_count));
  AddMember(result, "file_size_in_bytes", SerializeLong(data_file.file_size_in_bytes));
  AddMember(result, "split_offsets",
            SerializeOptionalWithValue(MakeSchemaOptional(MakeSchemaArray(MakeSchemaLong())),
                                       SerializeVectorLong(data_file.split_offsets)));
  AddMember(result, "equality_ids",
            SerializeOptionalWithValue(MakeSchemaOptional(MakeSchemaArray(MakeSchemaInt())),
                                       SerializeVectorInt(data_file.equality_ids)));
  AddMember(result, "file_format", SerializeString(data_file.file_format));
  AddMember(result, "file_path", SerializeString(data_file.file_path));
  AddMember(result, "content", SerializeInt(static_cast<int>(data_file.content)));
  AddMember(result, "referenced_data_file", SerializeOptionalString(data_file.referenced_data_file));
  AddMember(result, "sort_order_id", SerializeOptionalInt(data_file.sort_order_id));
  AddMember(
      result, "column_sizes",
      SerializeOptionalWithValue(MakeSchemaOptional(MakeSchemaMap("k117_v118", MakeSchemaInt(), MakeSchemaLong())),
                                 SerializeMapIntLong(data_file.column_sizes, "k117_v118")));
  AddMember(
      result, "value_counts",
      SerializeOptionalWithValue(MakeSchemaOptional(MakeSchemaMap("k119_v120", MakeSchemaInt(), MakeSchemaLong())),
                                 SerializeMapIntLong(data_file.value_counts, "k119_v120")));
  AddMember(
      result, "null_value_counts",
      SerializeOptionalWithValue(MakeSchemaOptional(MakeSchemaMap("k121_v122", MakeSchemaInt(), MakeSchemaLong())),
                                 SerializeMapIntLong(data_file.null_value_counts, "k121_v122")));
  AddMember(
      result, "lower_bounds",
      SerializeOptionalWithValue(MakeSchemaOptional(MakeSchemaMap("k126_v127", MakeSchemaInt(), MakeSchemaBytes())),
                                 SerializeMapIntBytes(data_file.lower_bounds, "k126_v127")));
  AddMember(
      result, "upper_bounds",
      SerializeOptionalWithValue(MakeSchemaOptional(MakeSchemaMap("k129_v130", MakeSchemaInt(), MakeSchemaBytes())),
                                 SerializeMapIntBytes(data_file.upper_bounds, "k129_v130")));
  AddMember(result, "partition", SerializePartition(partition_spec, data_file.partition_tuple));
  avro::GenericDatum datum(result.schema(), result);
  return datum;
}

avro::GenericDatum SerializeManifestEntry(const std::vector<PartitionKeyField>& partition_spec,
                                          const ManifestEntry& entry) {
  const auto schema = MakeSchemaManifestEntry(partition_spec);
  avro::GenericDatum result(schema.root());
  auto& record = result.value<avro::GenericRecord>();
  AddMember(record, "status", SerializeInt(static_cast<int>(entry.status)));
  AddMember(record, "snapshot_id", SerializeOptionalLong(entry.snapshot_id));
  AddMember(record, "sequence_number", SerializeOptionalLong(entry.sequence_number));
  AddMember(record, "file_sequence_number", SerializeOptionalLong(entry.file_sequence_number));
  AddMember(record, "data_file", SerializeDataFile(partition_spec, entry.data_file));
  return result;
}

}  // namespace

Manifest ReadManifestEntries(std::istream& input) {
  if (!input) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": input is invalid");
  }

  auto istream = avro::istreamInputStream(input);
  avro::DataFileReader<avro::GenericDatum> data_file_reader(std::move(istream));
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
  while (true) {
    avro::GenericDatum manifest_entry(data_file_reader.dataSchema());
    if (data_file_reader.read(manifest_entry)) {
      ManifestEntry entry = Deserialize<ManifestEntry>(manifest_entry);
      result.entries.emplace_back(std::move(entry));
    } else {
      break;
    }
  }
  return result;
}

Manifest ReadManifestEntries(const std::string& data) {
  std::stringstream ss(data);
  return ReadManifestEntries(ss);
}

std::string WriteManifestEntries(const Manifest& manifest, const std::vector<PartitionKeyField>& partition_spec) {
  static constexpr size_t bufferSize = 1024 * 1024;

  std::stringstream ss;
  auto ostream = avro::ostreamOutputStream(ss, bufferSize);
  avro::DataFileWriter<avro::GenericDatum> writer(
      std::move(ostream), avro::ValidSchema(MakeSchemaManifestEntry(partition_spec)), manifest.metadata);

  for (auto& man_entry : manifest.entries) {
    writer.write(SerializeManifestEntry(partition_spec, man_entry));
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
