#include "iceberg/src/table_metadata.h"

#include <functional>
#include <stdexcept>

#include "iceberg/src/field.h"
#include "rapidjson/document.h"

namespace iceberg {

namespace {
void ProcessArray(const rapidjson::Value& array,
                  std::function<void(const rapidjson::Value&)> callback) {
  if (!array.IsArray()) {
    throw std::runtime_error("ProcessArray: !array.IsArray()");
  }
  for (const auto& elem : array.GetArray()) {
    callback(elem);
  }
}

std::string ExtractStringField(const rapidjson::Value& document,
                               const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error("ExtractStringField: !document.HasMember(" +
                             field_name + ")");
  }
  if (!document[c_str].IsString()) {
    throw std::runtime_error("ExtractStringField: !document[" + field_name +
                             "].IsString())");
  }
  return std::string(document[c_str].GetString(),
                     document[c_str].GetStringLength());
}

int64_t ExtractInt64Field(const rapidjson::Value& document,
                          const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error("ExtractInt64Field: !document.HasMember(" +
                             field_name + ")");
  }
  if (!document[c_str].IsInt64()) {
    throw std::runtime_error("ExtractInt64Field: !document[" + field_name +
                             "].IsString())");
  }
  return document[c_str].GetInt64();
}

std::optional<int64_t> ExtractOptionalInt64Field(
    const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    return std::nullopt;
  }
  if (!document[c_str].IsInt64()) {
    return std::nullopt;
  }
  return document[c_str].GetInt64();
}

int32_t ExtractInt32Field(const rapidjson::Value& document,
                          const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error("ExtractInt32Field: !document.HasMember(" +
                             field_name + ")");
  }
  if (!document[c_str].IsInt()) {
    throw std::runtime_error("ExtractInt32Field: !document[" + field_name +
                             "].IsString())");
  }
  return document[c_str].GetInt();
}

bool ExtractBooleanField(const rapidjson::Value& document,
                         const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error("ExtractBooleanField: !document.HasMember(" +
                             field_name + ")");
  }
  if (!document[c_str].IsBool()) {
    throw std::runtime_error("ExtractBooleanField: !document[" + field_name +
                             "].IsString())");
  }
  return document[c_str].GetBool();
}

Field JsonToField(const rapidjson::Value& document) {
  Field result;
  result.id = ExtractInt32Field(document, "id");
  result.name = ExtractStringField(document, "name");
  result.is_required = ExtractBooleanField(document, "required");
  result.type = StringToDataType(ExtractStringField(document, "type"));
  return result;
}

std::vector<Field> ExtractSchemaFields(const rapidjson::Value& document,
                                       const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error("ExtractSchemaFields: !document.HasMember(" +
                             field_name + ")");
  }

  std::vector<Field> result;
  ProcessArray(document[c_str],
               [&result](const rapidjson::Value& elem) mutable {
                 result.emplace_back(JsonToField(elem));
               });
  return result;
}

Schema JsonToSchema(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error("JsonToSchema: !document.IsObject()");
  }

  int32_t schema_id = ExtractInt32Field(document, "schema-id");
  std::vector<Field> fields = ExtractSchemaFields(document, "fields");

  return Schema(schema_id, fields);
}

std::vector<Schema> ExtractSchemas(const rapidjson::Value& document) {
  const std::string field_name = "schemas";
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error("ExtractSchemas: !document.HasMember(" +
                             field_name + ")");
  }
  std::vector<Schema> result;
  ProcessArray(document[c_str],
               [&result](const rapidjson::Value& elem) mutable {
                 result.emplace_back(JsonToSchema(elem));
               });
  return result;
}

std::map<std::string, std::string> JsonToStringMap(
    const rapidjson::Value& document) {
  std::map<std::string, std::string> result;
  if (!document.IsObject()) {
    throw std::runtime_error("JsonToStringMap:!document.IsObject()");
  }
  for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it) {
    if (!it->name.IsString()) {
      throw std::runtime_error("JsonToStringMap:!it->name.IsString()");
    }
    if (!it->value.IsString()) {
      throw std::runtime_error("JsonToStringMap:!it->value.IsString()");
    }
    result.emplace(it->name.GetString(), it->value.GetString());
  }
  return result;
}

std::map<std::string, std::string> ExtractStringMap(
    const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error("ExtractStringMap:!document.HasMember(" +
                             field_name + ")");
  }
  return JsonToStringMap(document[c_str]);
}

Snapshot JsonToSnapshot(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error("JsonToSnapshot: !document.IsObject()");
  }

  int64_t snapshot_id = ExtractInt64Field(document, "snapshot-id");
  std::optional<int64_t> parent_snapshot_id =
      ExtractOptionalInt64Field(document, "parent-snapshot-id");
  int64_t sequence_number = ExtractInt64Field(document, "sequence-number");
  int64_t timestamp_ms = ExtractInt64Field(document, "timestamp-ms");
  std::string manifest_list = ExtractStringField(document, "manifest-list");
  std::map<std::string, std::string> summary =
      ExtractStringMap(document, "summary");
  if (!summary.contains("operation")) {
    throw std::runtime_error("JsonToSnapshot:!summary.contains(\"operation\")");
  }
  std::optional<int64_t> schema_id =
      ExtractOptionalInt64Field(document, "schema-id");
  return Snapshot{.snapshot_id = snapshot_id,
                  .parent_snapshot_id = parent_snapshot_id,
                  .sequence_number = sequence_number,
                  .timestamp_ms = timestamp_ms,
                  .manifest_list = std::move(manifest_list),
                  .summary = std::move(summary),
                  .schema_id = schema_id};
}

std::optional<std::vector<Snapshot>> ExtractSnapshots(
    const rapidjson::Value& document) {
  const std::string field_name = "snapshots";
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    return std::nullopt;
  }
  std::vector<Snapshot> result;
  ProcessArray(document[c_str],
               [&result](const rapidjson::Value& elem) mutable {
                 result.emplace_back(JsonToSnapshot(elem));
               });
  return result;
}

std::pair<int64_t, int64_t> JsonToSnapshotLogEntry(
    const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error("JsonToSchema: !document.IsObject()");
  }

  int64_t timestamp_ms = ExtractInt64Field(document, "timestamp-ms");
  int64_t snapshot_id = ExtractInt64Field(document, "snapshot-id");

  return std::make_pair(timestamp_ms, snapshot_id);
}

std::optional<std::vector<std::pair<int64_t, int64_t>>> ExtractSnapshotLog(
    const rapidjson::Value& document) {
  const std::string field_name = "snapshot-log";
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    return std::nullopt;
  }
  std::vector<std::pair<int64_t, int64_t>> result;
  ProcessArray(document[c_str],
               [&result](const rapidjson::Value& elem) mutable {
                 result.emplace_back(JsonToSnapshotLogEntry(elem));
               });
  return result;
}

std::pair<int64_t, std::string> JsonToMetadataLogEntry(
    const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error("JsonToMetadataLogEntry: !document.IsObject()");
  }

  int64_t timestamp_ms = ExtractInt64Field(document, "timestamp-ms");
  std::string metadata_file = ExtractStringField(document, "metadata-file");

  return std::make_pair(timestamp_ms, std::move(metadata_file));
}

std::optional<std::vector<std::pair<int64_t, std::string>>> ExtractMetadataLog(
    const rapidjson::Value& document) {
  const std::string field_name = "metadata-log";
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    return std::nullopt;
  }
  std::vector<std::pair<int64_t, std::string>> result;
  ProcessArray(document[c_str],
               [&result](const rapidjson::Value& elem) mutable {
                 result.emplace_back(JsonToMetadataLogEntry(elem));
               });
  return result;
}

std::optional<std::map<std::string, std::string>> ExtractProperties(
    const rapidjson::Value& document) {
  const std::string field_name = "properties";
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    return std::nullopt;
  }
  return JsonToStringMap(document[c_str]);
}
}  // namespace

std::optional<std::string> TableMetadataV2::GetCurrentManifestListPath() const {
  if (!current_snapshot_id.has_value() || !snapshots.has_value()) {
    return std::nullopt;
  }
  for (const auto& snapshot : *snapshots) {
    if (snapshot.snapshot_id == current_snapshot_id.value()) {
      return snapshot.manifest_list;
    }
  }
  return std::nullopt;
}

Schema TableMetadataV2::GetCurrentSchema() const {
  if (!current_snapshot_id.has_value() || !snapshots.has_value()) {
    throw std::runtime_error("GetCurrentSchema: no current snapshot");
  }
  std::optional<int64_t> schema_id;
  for (const auto& snapshot : *snapshots) {
    if (snapshot.snapshot_id == current_snapshot_id.value()) {
      if (!snapshot.schema_id.has_value()) {
        throw std::runtime_error("GetCurrentSchema: no schema id");
      }
      schema_id = snapshot.schema_id.value();
    }
  }
  if (!schema_id.has_value()) {
    throw std::runtime_error("GetCurrentSchema: no current snapshot");
  }
  for (const auto& schema : schemas) {
    if (schema.GetSchemaId() == schema_id.value()) {
      return schema;
    }
  }
  throw std::runtime_error(
      "GetCurrentSchema: no schema with current schema id");
}

TableMetadataV2 TableMetadataV2Builder::Build() && {
#define ASSERT_HAS_VALUE(field)                                       \
  do {                                                                \
    if (!field.has_value()) {                                         \
      throw std::runtime_error("TableMetadataV2Builder::Build(): !" + \
                               std::string(#field) + ".has_value()"); \
    }                                                                 \
  } while (false)

  ASSERT_HAS_VALUE(table_uuid);
  ASSERT_HAS_VALUE(location);
  ASSERT_HAS_VALUE(last_sequence_number);
  ASSERT_HAS_VALUE(last_updated_ms);
  ASSERT_HAS_VALUE(last_column_id);
  ASSERT_HAS_VALUE(schemas);
  ASSERT_HAS_VALUE(current_schema_id);
  ASSERT_HAS_VALUE(default_spec_id);
  ASSERT_HAS_VALUE(last_partition_id);
  ASSERT_HAS_VALUE(default_sort_order_id);

#undef ASSERT_HAS_VALUE

  return TableMetadataV2(
      std::move(table_uuid.value()), std::move(location.value()),
      last_sequence_number.value(), last_updated_ms.value(),
      last_column_id.value(), schemas.value(), current_schema_id.value(),
      default_spec_id.value(), last_partition_id.value(), std::move(properties),
      current_snapshot_id, std::move(snapshots), std::move(snapshot_log),
      std::move(metadata_log), default_sort_order_id.value());
}

TableMetadataV2 MakeTableMetadataV2(const std::string& json) {
  TableMetadataV2Builder builder;

  rapidjson::Document document;
  document.Parse(json.c_str(), json.size());
  if (!document.IsObject()) {
    throw std::runtime_error("MakeTableMetadataV2: !document.IsObject()");
  }

  builder.table_uuid = ExtractStringField(document, "table-uuid");
  builder.location = ExtractStringField(document, "location");
  builder.last_sequence_number =
      ExtractInt64Field(document, "last-sequence-number");
  builder.last_updated_ms = ExtractInt64Field(document, "last-updated-ms");
  builder.last_column_id = ExtractInt32Field(document, "last-column-id");
  builder.schemas = ExtractSchemas(document);
  builder.current_schema_id = ExtractInt32Field(document, "current-schema-id");
  builder.default_spec_id = ExtractInt32Field(document, "default-spec-id");
  builder.last_partition_id = ExtractInt32Field(document, "last-partition-id");
  if (auto maybe_value = ExtractProperties(document); maybe_value.has_value()) {
    builder.properties = std::move(maybe_value.value());
  }
  if (auto maybe_value =
          ExtractOptionalInt64Field(document, "current-snapshot-id");
      maybe_value.has_value()) {
    builder.current_snapshot_id = maybe_value.value();
  }
  if (auto maybe_value = ExtractSnapshots(document); maybe_value.has_value()) {
    builder.snapshots = std::move(maybe_value.value());
  }
  if (auto maybe_value = ExtractSnapshotLog(document);
      maybe_value.has_value()) {
    builder.snapshot_log = std::move(maybe_value.value());
  }
  if (auto maybe_value = ExtractMetadataLog(document);
      maybe_value.has_value()) {
    builder.metadata_log = std::move(maybe_value.value());
  }
  builder.default_sort_order_id =
      ExtractInt32Field(document, "default-sort-order-id");
  return std::move(builder).Build();
}

}  // namespace iceberg
