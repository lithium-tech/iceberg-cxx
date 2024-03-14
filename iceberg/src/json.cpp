#include "iceberg/src/json.h"

#include <cassert>
#include <stdexcept>

#include "iceberg/src/manifest_entry.h"
#include "iceberg/src/manifest_metadata.h"
#include "iceberg/src/scan.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace iceberg {

namespace {

class Serializer {
 public:
  using Allocator = rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator>;

  rapidjson::Value SerializeScanMetadata(const ScanMetadata& scan_metadata);

  rapidjson::Value SerializeSchema(const Schema& schema);

  rapidjson::Value SerializeFields(const std::vector<Field>& fields);

  rapidjson::Value SerializeField(const Field& field);

  rapidjson::Value SerializeManifestEntries(
      const std::vector<ManifestEntry>& entry);

  rapidjson::Value SerializeManifestEntry(const ManifestEntry& entry);

  rapidjson::Value SerializeDataFile(const DataFile& data_file);

  template <typename T>
  rapidjson::Value SerializeVector(const std::vector<T>& values);

 private:
  Allocator allocator_;
};

template <typename T>
rapidjson::Value Serializer::SerializeVector(const std::vector<T>& values) {
  rapidjson::Value result(rapidjson::kArrayType);
  for (auto value : values) {
    result.PushBack(value, allocator_);
  }
  return result;
}

rapidjson::Value Serializer::SerializeDataFile(const DataFile& data_file) {
  rapidjson::Value result(rapidjson::kObjectType);
  result.AddMember("file_path",
                   rapidjson::Value(data_file.file_path.c_str(), allocator_),
                   allocator_);
  result.AddMember("file_size_in_bytes", data_file.file_size_in_bytes,
                   allocator_);
  result.AddMember("content", static_cast<uint8_t>(data_file.content),
                   allocator_);
  if (data_file.content == DataFile::Content::kEqualityDelete) {
    assert(data_file.equality_ids.has_value());
    result.AddMember("equality_ids",
                     SerializeVector(data_file.equality_ids.value()),
                     allocator_);
  } else if (data_file.content == DataFile::Content::kData) {
    assert(data_file.split_offsets.has_value());
    result.AddMember("split-offsets",
                     SerializeVector(data_file.split_offsets.value()),
                     allocator_);
  }
  return result;
}

rapidjson::Value Serializer::SerializeManifestEntry(
    const ManifestEntry& entry) {
  rapidjson::Value result(rapidjson::kObjectType);
  assert(entry.sequence_number.has_value());
  result.AddMember("sequence_number", entry.sequence_number.value(),
                   allocator_);
  result.AddMember("data_file", SerializeDataFile(entry.data_file), allocator_);
  return result;
}

rapidjson::Value Serializer::SerializeManifestEntries(
    const std::vector<ManifestEntry>& entries) {
  rapidjson::Value result(rapidjson::kArrayType);
  for (const auto& entry : entries) {
    result.PushBack(SerializeManifestEntry(entry), allocator_);
  }
  return result;
}

rapidjson::Value Serializer::SerializeField(const Field& field) {
  rapidjson::Value result(rapidjson::kObjectType);
  result.AddMember("id", field.id, allocator_);
  result.AddMember("is_required", field.is_required, allocator_);
  result.AddMember("name", rapidjson::Value(field.name.c_str(), allocator_),
                   allocator_);
  result.AddMember("type",
                   rapidjson::Value(field.type->ToString().c_str(), allocator_),
                   allocator_);
  return result;
}

rapidjson::Value Serializer::SerializeFields(const std::vector<Field>& fields) {
  rapidjson::Value result(rapidjson::kArrayType);
  for (const auto& field : fields) {
    result.PushBack(SerializeField(field), allocator_);
  }
  return result;
}

rapidjson::Value Serializer::SerializeSchema(const Schema& schema) {
  rapidjson::Value result(rapidjson::kObjectType);
  result.AddMember("id", schema.GetSchemaId(), allocator_);
  result.AddMember("fields", SerializeFields(schema.GetFields()), allocator_);
  return result;
}

rapidjson::Value Serializer::SerializeScanMetadata(
    const ScanMetadata& scan_metadata) {
  rapidjson::Value result(rapidjson::kObjectType);
  result.AddMember("schema", SerializeSchema(scan_metadata.schema), allocator_);
  result.AddMember("manifest_entries",
                   SerializeManifestEntries(scan_metadata.entries), allocator_);
  return result;
}

}  // namespace

std::string ScanMetadataToJSONString(const ScanMetadata& scan_metadata) {
  Serializer serializer;
  auto json_result = serializer.SerializeScanMetadata(scan_metadata);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  json_result.Accept(writer);
  return std::string(buffer.GetString(), buffer.GetLength());
}

}  // namespace iceberg
