#include "iceberg/manifest_entry.h"

#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>

#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <type_traits>

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Generic.hh"
#include "avro/GenericDatum.hh"
#include "avro/Schema.hh"
#include "avro/Types.hh"
#include "avro/ValidSchema.hh"
#include "iceberg/table_metadata.h"
#include "rapidjson/document.h"

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
DataFile::PartitionInfo Deserialize(const avro::GenericDatum& datum) {
  if (datum.type() != avro::AVRO_RECORD) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": unexpected datum type");
  }
  const auto& record = datum.value<avro::GenericRecord>();
  DataFile::PartitionInfo result;
  const auto& schema = record.schema();
  for (size_t i = 0; i < schema->names(); ++i) {
    auto name = schema->nameAt(i);

    const auto& field = record.fieldAt(record.fieldIndex(name));
    if (field.type() == avro::AVRO_INT) {
      result.fields.emplace_back(DataFile::PartitionInfoField{.name = name, .value = field.value<int>()});
    } else {
      throw std::runtime_error("Partitioning for types other than int is not implemented");
      // TODO(gmusya): support physical types
      // TODO(gmusya): also save logical type
    }
  }
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
  data_file.column_sizes = Extract<std::map<int32_t, int64_t>>(record, "column_sizes");
  data_file.value_counts = Extract<std::map<int32_t, int64_t>>(record, "value_counts");
  data_file.split_offsets = Extract<std::vector<int64_t>>(record, "split_offsets");
  data_file.null_value_counts = Extract<std::map<int32_t, int64_t>>(record, "null_value_counts");
  data_file.lower_bounds = Extract<std::map<int32_t, std::vector<uint8_t>>>(record, "lower_bounds");
  data_file.upper_bounds = Extract<std::map<int32_t, std::vector<uint8_t>>>(record, "upper_bounds");
  data_file.partition_info = Extract<DataFile::PartitionInfo>(record, "partition");
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

avro::ValidSchema MakeSchemaBytes() { return avro::ValidSchema(avro::BytesSchema()); }

avro::ValidSchema MakeSchemaString() { return avro::ValidSchema(avro::StringSchema()); }

avro::ValidSchema MakeSchemaInt() { return avro::ValidSchema(avro::IntSchema()); }

avro::ValidSchema MakeSchemaLong() { return avro::ValidSchema(avro::LongSchema()); }

avro::ValidSchema MakeSchemaPartition(const std::vector<PartitionField>& partition_spec) {
  avro::RecordSchema schema("r102");
  for (const auto& field : partition_spec) {
    AddField(schema, field.name, MakeSchemaOptional(MakeSchemaInt()));
  }
  return avro::ValidSchema(schema);
}

avro::ValidSchema MakeSchemaDataFile(const std::vector<PartitionField>& partition_spec) {
  avro::RecordSchema schema("r2");
  AddField(schema, "file_path", MakeSchemaString());
  AddField(schema, "file_format", MakeSchemaString());
  AddField(schema, "content", MakeSchemaInt());
  AddField(schema, "file_size_in_bytes", MakeSchemaLong());
  AddField(schema, "record_count", MakeSchemaLong());
  AddField(schema, "split_offsets", MakeSchemaOptional(MakeSchemaArray(MakeSchemaLong())));
  AddField(schema, "sort_order_id", MakeSchemaOptional(MakeSchemaInt()));
  AddField(schema, "column_sizes", MakeSchemaOptional(MakeSchemaMap("k117_v118", MakeSchemaInt(), MakeSchemaLong())));
  AddField(schema, "value_counts", MakeSchemaOptional(MakeSchemaMap("k119_v120", MakeSchemaInt(), MakeSchemaLong())));
  AddField(schema, "null_value_counts",
           MakeSchemaOptional(MakeSchemaMap("k121_v122", MakeSchemaInt(), MakeSchemaLong())));
  AddField(schema, "lower_bounds", MakeSchemaOptional(MakeSchemaMap("k126_v127", MakeSchemaInt(), MakeSchemaBytes())));
  AddField(schema, "upper_bounds", MakeSchemaOptional(MakeSchemaMap("k129_v130", MakeSchemaInt(), MakeSchemaBytes())));
  AddField(schema, "partition", MakeSchemaPartition(partition_spec));
  return avro::ValidSchema(schema);
}

avro::ValidSchema MakeSchemaManifestEntry(const std::vector<PartitionField>& partition_spec) {
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

avro::GenericDatum SerializeInt(int value) { return avro::GenericDatum(value); }
avro::GenericDatum SerializeLong(int64_t value) { return avro::GenericDatum(value); }
avro::GenericDatum SerializeString(std::string value) { return avro::GenericDatum(std::move(value)); }
avro::GenericDatum SerializeBytes(std::vector<uint8_t> value) { return avro::GenericDatum(std::move(value)); }

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

avro::GenericDatum SerializePartition(const std::vector<PartitionField>& partition_spec,
                                      const DataFile::PartitionInfo& partition) {
  const auto partition_schema = MakeSchemaPartition(partition_spec);
  avro::GenericRecord record(partition_schema.root());
  for (const auto& info : partition.fields) {
    if (record.hasField(info.name)) {
      // TODO(gmusya): validate that spec has same type as value in DataFile::PartitionInfo
      record.setFieldAt(record.fieldIndex(info.name), SerializeOptionalInt(info.value));
    }
  }

  return avro::GenericDatum(record.schema(), std::move(record));
}

avro::GenericDatum SerializeDataFile(const std::vector<PartitionField>& partition_spec, const DataFile& data_file) {
  const auto data_schema = MakeSchemaDataFile(partition_spec);
  avro::GenericRecord result(data_schema.root());
  AddMember(result, "record_count", SerializeLong(data_file.record_count));
  AddMember(result, "file_size_in_bytes", SerializeLong(data_file.file_size_in_bytes));
  AddMember(result, "split_offsets",
            SerializeOptionalWithValue(MakeSchemaOptional(MakeSchemaArray(MakeSchemaLong())),
                                       SerializeVectorLong(data_file.split_offsets)));
  AddMember(result, "file_format", SerializeString(data_file.file_format));
  AddMember(result, "file_path", SerializeString(data_file.file_path));
  AddMember(result, "content", SerializeInt(static_cast<int>(data_file.content)));
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
  AddMember(result, "partition", SerializePartition(partition_spec, data_file.partition_info));
  avro::GenericDatum datum(result.schema(), result);
  return datum;
}

avro::GenericDatum SerializeManifestEntry(const std::vector<PartitionField>& partition_spec,
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

Manifest ReadManifestEntries(std::istream& input, const std::vector<PartitionField>& partition_spec) {
  if (!input) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": input is invalid");
  }

  auto schema = MakeSchemaManifestEntry(partition_spec);
  auto istream = avro::istreamInputStream(input);
  avro::DataFileReader<avro::GenericDatum> data_file_reader(std::move(istream), schema);
  Manifest result;
  avro::GenericDatum manifest_entry(schema);

  const auto& meta = data_file_reader.metadata();

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

  while (data_file_reader.read(manifest_entry)) {
    ManifestEntry entry = Deserialize<ManifestEntry>(manifest_entry);
    result.entries.emplace_back(std::move(entry));
  }
  return result;
}

Manifest ReadManifestEntries(const std::string& data, const std::vector<PartitionField>& partition_spec) {
  std::stringstream ss(data);
  return ReadManifestEntries(ss, partition_spec);
}

std::string WriteManifestEntries(const Manifest& manifest, const std::vector<PartitionField>& partition_spec) {
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
