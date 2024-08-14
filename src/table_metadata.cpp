#include "src/table_metadata.h"

#include <functional>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

#include "src/nested_field.h"
#include "src/type.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace iceberg {

namespace {

struct Names {
  static constexpr const char* format_version = "format-version";
  static constexpr const char* table_uuid = "table-uuid";
  static constexpr const char* location = "location";
  static constexpr const char* last_sequence_number = "last-sequence-number";
  static constexpr const char* last_updated_ms = "last-updated-ms";
  static constexpr const char* last_column_id = "last-column-id";
  static constexpr const char* current_schema_id = "current-schema-id";
  static constexpr const char* schemas = "schemas";
  static constexpr const char* partition_specs = "partition-specs";
  static constexpr const char* default_spec_id = "default-spec-id";
  static constexpr const char* last_partition_id = "last-partition-id";
  static constexpr const char* properties = "properties";
  static constexpr const char* current_snapshot_id = "current-snapshot-id";
  static constexpr const char* snapshots = "snapshots";
  static constexpr const char* snapshot_log = "snapshot-log";
  static constexpr const char* metadata_log = "metadata-log";
  static constexpr const char* sort_orders = "sort-orders";
  static constexpr const char* default_sort_order_id = "default-sort-order-id";
  static constexpr const char* refs = "refs";
  static constexpr const char* fields = "fields";
  static constexpr const char* type = "type";
  static constexpr const char* list = "list";
  static constexpr const char* element_id = "element-id";
  static constexpr const char* element_required = "element-required";
  static constexpr const char* element = "element";
  static constexpr const char* struct_ = "struct";
  static constexpr const char* schema_id = "schema-id";
  static constexpr const char* id = "id";
  static constexpr const char* name = "name";
  static constexpr const char* required = "required";
  static constexpr const char* source_id = "source-id";
  static constexpr const char* field_id = "field-id";
  static constexpr const char* transform = "transform";
  static constexpr const char* spec_id = "spec-id";
  static constexpr const char* snapshot_id = "snapshot-id";
  static constexpr const char* parent_snapshot_id = "parent-snapshot-id";
  static constexpr const char* sequence_number = "sequence-number";
  static constexpr const char* timestamp_ms = "timestamp-ms";
  static constexpr const char* operation = "operation";
  static constexpr const char* summary = "summary";
  static constexpr const char* manifest_list = "manifest-list";
  static constexpr const char* metadata_file = "metadata-file";
  static constexpr const char* order_id = "order-id";
  static constexpr const char* direction = "direction";
  static constexpr const char* null_order = "null-order";
  static constexpr const char* asc = "asc";
  static constexpr const char* desc = "desc";
  static constexpr const char* nulls_first = "nulls-first";
  static constexpr const char* nulls_last = "nulls-last";
  static constexpr const char* min_snapshots_to_keep = "min-snapshots-to-keep";
  static constexpr const char* max_snapshot_age_ms = "max-snapshot-age-ms";
  static constexpr const char* max_ref_age_ms = "max-ref-age-ms";
};

auto Ref(const char* s) { return rapidjson::StringRef(s, strlen(s)); }

void ProcessArray(const rapidjson::Value& array, std::function<void(const rapidjson::Value&)> callback) {
  if (!array.IsArray()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !array.IsArray()");
  }
  for (const auto& elem : array.GetArray()) {
    callback(elem);
  }
}

template <typename Allocator>
class WriterContext {
 public:
  explicit WriterContext(Allocator& allocator) : allocator_(allocator) {}

  void WriteStringField(rapidjson::Value& doc, const std::string& field_name, const std::string& str_value) {
    strings_.emplace_back(std::make_shared<std::string>(field_name));
    auto& name = *strings_.back();
    strings_.emplace_back(std::make_shared<std::string>(str_value));
    auto& value = *strings_.back();

    doc.AddMember(rapidjson::StringRef(name.data(), name.size()), rapidjson::StringRef(value.data(), value.size()),
                  GetAllocator());
  }

  template <typename T>
  auto WriteIntField(rapidjson::Value& doc, std::string_view field_name, T value) {
    doc.AddMember(rapidjson::StringRef(field_name.data(), field_name.size()), value, GetAllocator());
  }

  void WriteDataType(rapidjson::Value& doc, const types::Type& type) {
    if (type.IsPrimitiveType()) {
      // TODO(chertus): decimal
      WriteStringField(doc, Names::type, type.ToString());
    } else if (type.IsListType()) {
      rapidjson::Value element(rapidjson::kObjectType);

      WriteStringField(element, Names::type, Names::list);
      auto* list_type = static_cast<const types::ListType*>(&type);
      WriteIntField(element, Names::element_id, list_type->ElementId());
      WriteIntField(element, Names::element_required, list_type->ElementRequired());
      if (!list_type->ElementType()) {
        throw std::runtime_error(std::string(__FUNCTION__) + ": no type");
      }
      WriteDataType(element, *list_type->ElementType());

      doc.AddMember(Ref(Names::element), element.Move(), GetAllocator());
    }
  }

  void WriteField(rapidjson::Value& doc, const types::NestedField& field) {
    WriteIntField(doc, Names::id, field.field_id);
    WriteStringField(doc, Names::name, field.name);
    WriteIntField(doc, Names::required, field.is_required);
    if (!field.type) {
      throw std::runtime_error(std::string(__FUNCTION__) + ": no type");
    }
    WriteDataType(doc, *field.type);
  }

  void WriteSchemas(rapidjson::Value& doc, const std::vector<std::shared_ptr<Schema>>& array) {
    rapidjson::Value schemas(rapidjson::kArrayType);
    for (auto& s : array) {
      rapidjson::Value schema(rapidjson::kObjectType);

      WriteStringField(schema, Names::type, Names::struct_);
      WriteIntField(schema, Names::schema_id, s->SchemaId());

      rapidjson::Value fields(rapidjson::kArrayType);
      for (auto column : s->Columns()) {
        rapidjson::Value field(rapidjson::kObjectType);
        WriteField(field, column);
        fields.PushBack(field.Move(), GetAllocator());
      }
      schema.AddMember(Ref(Names::fields), fields.Move(), GetAllocator());

      schemas.PushBack(schema.Move(), GetAllocator());
    }
    doc.AddMember(Ref(Names::schemas), schemas.Move(), GetAllocator());
  }

  void WritePartitionField(rapidjson::Value& doc, const PartitionField& field) {
    WriteIntField(doc, Names::source_id, field.source_id);
    WriteIntField(doc, Names::field_id, field.field_id);
    WriteStringField(doc, Names::name, field.name);
    WriteStringField(doc, Names::transform, field.transform);
  }

  void WritePartitionSpec(rapidjson::Value& doc, const std::vector<std::shared_ptr<PartitionSpec>>& array) {
    rapidjson::Value part_specs(rapidjson::kArrayType);
    for (auto& s : array) {
      rapidjson::Value spec(rapidjson::kObjectType);

      WriteIntField(spec, Names::spec_id, s->spec_id);

      rapidjson::Value fields(rapidjson::kArrayType);
      for (auto pf : s->fields) {
        rapidjson::Value field(rapidjson::kObjectType);
        WritePartitionField(field, pf);
        fields.PushBack(field.Move(), GetAllocator());
      }
      spec.AddMember(Ref(Names::fields), fields.Move(), GetAllocator());

      part_specs.PushBack(spec.Move(), GetAllocator());
    }
    doc.AddMember(Ref(Names::partition_specs), part_specs.Move(), GetAllocator());
  }

  void WriteStringMap(rapidjson::Value& doc, const std::map<std::string, std::string>& values) {
    for (auto& [key, value] : values) {
      WriteStringField(doc, key, value);
    }
  }

  void WriteSnapshot(rapidjson::Value& document, const Snapshot& snap) {
    WriteIntField(document, Names::sequence_number, snap.sequence_number);
    WriteIntField(document, Names::snapshot_id, snap.snapshot_id);
    if (snap.parent_snapshot_id) {
      WriteIntField(document, Names::parent_snapshot_id, *snap.parent_snapshot_id);
    }
    WriteIntField(document, Names::timestamp_ms, snap.timestamp_ms);

    if (!snap.summary.contains(Names::operation)) {
      throw std::runtime_error(std::string(__FUNCTION__) + ": no operation in summary");
    }

    rapidjson::Value summary(rapidjson::kObjectType);
    WriteStringMap(summary, snap.summary);
    document.AddMember(Ref(Names::summary), summary.Move(), GetAllocator());

    WriteStringField(document, Names::manifest_list, snap.manifest_list_location);
    if (snap.schema_id) {
      WriteIntField(document, Names::schema_id, *snap.schema_id);
    }
  }

  void WriteSnapshots(rapidjson::Value& doc, const std::vector<std::shared_ptr<Snapshot>>& snaps) {
    if (snaps.empty()) {
      return;
    }
    rapidjson::Value snapshots(rapidjson::kArrayType);
    for (auto s : snaps) {
      rapidjson::Value snap(rapidjson::kObjectType);
      WriteSnapshot(snap, *s);
      snapshots.PushBack(snap.Move(), GetAllocator());
    }
    doc.AddMember(Ref(Names::snapshots), snapshots.Move(), GetAllocator());
  }

  void WriteSnapshotLogEntry(rapidjson::Value& doc, const SnapshotLog& entry) {
    WriteIntField(doc, Names::timestamp_ms, entry.timestamp_ms);
    WriteIntField(doc, Names::snapshot_id, entry.snapshot_id);
  }

  void WriteSnapshotLog(rapidjson::Value& doc, const std::vector<SnapshotLog>& snap_log) {
    if (snap_log.empty()) {
      return;
    }
    rapidjson::Value log_array(rapidjson::kArrayType);
    for (auto snap : snap_log) {
      rapidjson::Value log(rapidjson::kObjectType);
      WriteSnapshotLogEntry(log, snap);
      log_array.PushBack(log.Move(), GetAllocator());
    }
    doc.AddMember(Ref(Names::snapshot_log), log_array.Move(), GetAllocator());
  }

  void WriteMetadataLogEntry(rapidjson::Value& doc, const MetadataLog& entry) {
    WriteIntField(doc, Names::timestamp_ms, entry.timestamp_ms);
    WriteStringField(doc, Names::metadata_file, entry.metadata_file);
  }

  void WriteMetadataLog(rapidjson::Value& doc, const std::vector<MetadataLog>& metadata_log) {
    if (metadata_log.empty()) {
      return;
    }
    rapidjson::Value log_array(rapidjson::kArrayType);
    for (auto meta : metadata_log) {
      rapidjson::Value log(rapidjson::kObjectType);
      WriteMetadataLogEntry(log, meta);
      log_array.PushBack(log.Move(), GetAllocator());
    }
    doc.AddMember(Ref(Names::metadata_log), log_array.Move(), GetAllocator());
  }

  void WriteProperties(rapidjson::Value& doc, const std::map<std::string, std::string>& values) {
    if (values.empty()) {
      return;
    }
    rapidjson::Value properties(rapidjson::kObjectType);
    WriteStringMap(properties, values);
    doc.AddMember(Ref(Names::properties), properties.Move(), GetAllocator());
  }

  void WriteSortField(rapidjson::Value& doc, const SortField& field) {
    WriteStringField(doc, Names::transform, field.transform);
    WriteIntField(doc, Names::source_id, field.source_id);
    WriteStringField(doc, Names::direction, (field.direction == SortDirection::kAsc ? Names::asc : Names::desc));
    WriteStringField(doc, Names::null_order,
                     (field.null_order == NullOrder::kNullsFirst ? Names::nulls_first : Names::nulls_last));
  }

  void WriteSortOrder(rapidjson::Value& doc, const std::vector<std::shared_ptr<SortOrder>>& array) {
    rapidjson::Value orders(rapidjson::kArrayType);
    for (auto& s : array) {
      rapidjson::Value spec(rapidjson::kObjectType);

      WriteIntField(spec, Names::order_id, s->order_id);

      rapidjson::Value fields(rapidjson::kArrayType);
      for (auto pf : s->fields) {
        rapidjson::Value field(rapidjson::kObjectType);
        WriteSortField(field, pf);
        fields.PushBack(field.Move(), GetAllocator());
      }
      spec.AddMember(Ref(Names::fields), fields.Move(), GetAllocator());

      orders.PushBack(spec.Move(), GetAllocator());
    }
    doc.AddMember(Ref(Names::sort_orders), orders.Move(), GetAllocator());
  }

  void WriteRefField(rapidjson::Value& doc, const std::string& name, const SnapshotRef& snap_ref) {
    rapidjson::Value ref(rapidjson::kObjectType);
    WriteIntField(ref, Names::snapshot_id, snap_ref.snapshot_id);
    WriteStringField(ref, Names::type, snap_ref.type);
    if (snap_ref.min_snapshots_to_keep) {
      WriteIntField(ref, Names::min_snapshots_to_keep, *snap_ref.min_snapshots_to_keep);
    }
    if (snap_ref.max_snapshot_age_ms) {
      WriteIntField(ref, Names::max_snapshot_age_ms, *snap_ref.max_snapshot_age_ms);
    }
    if (snap_ref.max_ref_age_ms) {
      WriteIntField(ref, Names::max_ref_age_ms, *snap_ref.max_ref_age_ms);
    }

    doc.AddMember(rapidjson::StringRef(name.data(), name.size()), ref.Move(), GetAllocator());
  }

  void WriteRefs(rapidjson::Value& doc, const std::map<std::string, SnapshotRef>& values) {
    if (values.empty()) {
      return;
    }

    rapidjson::Value refs(rapidjson::kObjectType);
    for (auto& [key, value] : values) {
      WriteRefField(refs, key, value);
    }

    doc.AddMember(Ref(Names::refs), refs.Move(), GetAllocator());
  }

 private:
  Allocator& allocator_;
  std::vector<std::shared_ptr<std::string>> strings_;

  auto& GetAllocator() { return allocator_; }
};

std::string ExtractStringField(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  }
  if (!document[c_str].IsString()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document[" + field_name + "].IsString())");
  }
  return std::string(document[c_str].GetString(), document[c_str].GetStringLength());
}

int64_t ExtractInt64Field(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  }
  if (!document[c_str].IsInt64()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document[" + field_name + "].IsString())");
  }
  return document[c_str].GetInt64();
}

std::optional<int32_t> ExtractOptionalInt32Field(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    return std::nullopt;
  }
  if (!document[c_str].IsInt()) {
    return std::nullopt;
  }
  return document[c_str].GetInt();
}

std::optional<int64_t> ExtractOptionalInt64Field(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    return std::nullopt;
  }
  if (!document[c_str].IsInt64()) {
    return std::nullopt;
  }
  return document[c_str].GetInt64();
}

int32_t ExtractInt32Field(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  }
  if (!document[c_str].IsInt()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document[" + field_name + "].IsString())");
  }
  return document[c_str].GetInt();
}

bool ExtractBooleanField(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  }
  if (!document[c_str].IsBool()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document[" + field_name + "].IsString())");
  }
  return document[c_str].GetBool();
}

std::shared_ptr<const types::Type> JsonToDataType(const rapidjson::Value& value) {
  if (value.IsString()) {
    std::string str = value.GetString();
    if (auto maybe_value = types::NameToPrimitiveType(str); maybe_value.has_value()) {
      return std::make_shared<types::PrimitiveType>(maybe_value.value());
    }
    if (str.starts_with("decimal")) {
      // decimal(P, S)
      std::stringstream ss(str);
      ss.ignore(std::string("decimal(").size());
      int32_t precision = -1;
      int32_t scale = -1;
      ss >> precision;
      ss.ignore(1);  // skip comma
      ss >> scale;
      return std::make_shared<types::DecimalType>(precision, scale);
    }
    throw std::runtime_error(std::string(__FUNCTION__) + ": unknown type '" + str + "'");
  }
  if (value.IsObject()) {
    if (!value.HasMember(Names::type)) {
      throw std::runtime_error(std::string(__FUNCTION__) + ": !value.HasMember(\"type\"");
    }

    std::string type = ExtractStringField(value, Names::type);
    if (type == Names::list) {
      int32_t element_field_id = ExtractInt32Field(value, Names::element_id);
      bool element_required = ExtractBooleanField(value, Names::element_required);

      if (!value.HasMember(Names::element)) {
        throw std::runtime_error(std::string(__FUNCTION__) + ": !value.HasMember(\"element\"");
      }
      std::shared_ptr<const types::Type> element_type = JsonToDataType(value[Names::element]);

      return std::make_shared<types::ListType>(element_field_id, element_required, element_type);
    }
  }
  throw std::runtime_error(std::string(__FUNCTION__) + ": unknown type");
}

types::NestedField JsonToField(const rapidjson::Value& document) {
  types::NestedField result;
  result.field_id = ExtractInt32Field(document, Names::id);
  result.name = ExtractStringField(document, Names::name);
  result.is_required = ExtractBooleanField(document, Names::required);

  if (!document.HasMember(Names::type)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": document.HasMember(\"type\")");
  }

  result.type = JsonToDataType(document[Names::type]);
  return result;
}

std::vector<types::NestedField> ExtractSchemaFields(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  }

  std::vector<types::NestedField> result;
  ProcessArray(document[c_str],
               [&result](const rapidjson::Value& elem) mutable { result.emplace_back(JsonToField(elem)); });
  return result;
}

std::shared_ptr<Schema> JsonToSchema(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.IsObject()");
  }

  int32_t schema_id = ExtractInt32Field(document, Names::schema_id);
  std::vector<types::NestedField> fields = ExtractSchemaFields(document, Names::fields);

  return std::make_shared<Schema>(schema_id, fields);
}

std::vector<std::shared_ptr<Schema>> ExtractSchemas(const rapidjson::Value& document) {
  static constexpr const char* field_name = Names::schemas;
  if (!document.HasMember(field_name)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.HasMember(" + std::string(field_name) + ")");
  }
  std::vector<std::shared_ptr<Schema>> result;
  ProcessArray(document[field_name],
               [&result](const rapidjson::Value& elem) mutable { result.emplace_back(JsonToSchema(elem)); });
  return result;
}

PartitionField JsonToPartitionField(const rapidjson::Value& document) {
  return PartitionField{.source_id = ExtractInt32Field(document, Names::source_id),
                        .field_id = ExtractInt32Field(document, Names::field_id),
                        .name = ExtractStringField(document, Names::name),
                        .transform = ExtractStringField(document, Names::transform)};
}

std::vector<PartitionField> ExtractPartitionFields(const rapidjson::Value& document) {
  static constexpr const char* field_name = Names::fields;
  if (!document.HasMember(field_name)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  }

  std::vector<PartitionField> result;
  ProcessArray(document[field_name],
               [&result](const rapidjson::Value& elem) mutable { result.emplace_back(JsonToPartitionField(elem)); });
  return result;
}

std::shared_ptr<PartitionSpec> JsonPartitionSpec(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.IsObject()");
  }

  int32_t schema_id = ExtractInt32Field(document, Names::spec_id);
  return std::make_shared<PartitionSpec>(PartitionSpec{schema_id, ExtractPartitionFields(document)});
}

std::vector<std::shared_ptr<PartitionSpec>> ExtractPartitionSpecs(const rapidjson::Value& document) {
  static constexpr const char* field_name = Names::partition_specs;
  if (!document.HasMember(field_name)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.HasMember(" + std::string(field_name) + ")");
  }
  std::vector<std::shared_ptr<PartitionSpec>> result;
  ProcessArray(document[field_name],
               [&result](const rapidjson::Value& elem) mutable { result.emplace_back(JsonPartitionSpec(elem)); });
  return result;
}

std::map<std::string, std::string> JsonToStringMap(const rapidjson::Value& document) {
  std::map<std::string, std::string> result;
  if (!document.IsObject()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.IsObject()");
  }
  for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it) {
    if (!it->name.IsString()) {
      throw std::runtime_error(std::string(__FUNCTION__) + ": !it->name.IsString()");
    }
    if (!it->value.IsString()) {
      throw std::runtime_error(std::string(__FUNCTION__) + ": !it->value.IsString()");
    }
    result.emplace(it->name.GetString(), it->value.GetString());
  }
  return result;
}

std::map<std::string, std::string> ExtractStringMap(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  }
  return JsonToStringMap(document[c_str]);
}

std::shared_ptr<Snapshot> JsonToSnapshot(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.IsObject()");
  }

  int64_t snapshot_id = ExtractInt64Field(document, Names::snapshot_id);
  std::optional<int64_t> parent_snapshot_id = ExtractOptionalInt64Field(document, Names::parent_snapshot_id);
  int64_t sequence_number = ExtractInt64Field(document, Names::sequence_number);
  int64_t timestamp_ms = ExtractInt64Field(document, Names::timestamp_ms);
  std::string manifest_list = ExtractStringField(document, Names::manifest_list);
  std::map<std::string, std::string> summary = ExtractStringMap(document, Names::summary);
  if (!summary.contains(Names::operation)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !summary.contains(\"operation\")");
  }
  std::optional<int64_t> schema_id = ExtractOptionalInt64Field(document, Names::schema_id);
  return std::make_shared<Snapshot>(Snapshot{.snapshot_id = snapshot_id,
                                             .parent_snapshot_id = parent_snapshot_id,
                                             .sequence_number = sequence_number,
                                             .timestamp_ms = timestamp_ms,
                                             .manifest_list_location = std::move(manifest_list),
                                             .summary = std::move(summary),
                                             .schema_id = schema_id});
}

std::optional<std::vector<std::shared_ptr<Snapshot>>> ExtractSnapshots(const rapidjson::Value& document) {
  static constexpr const char* field_name = Names::snapshots;
  if (!document.HasMember(field_name)) {
    return std::nullopt;
  }
  std::vector<std::shared_ptr<Snapshot>> result;
  ProcessArray(document[field_name],
               [&result](const rapidjson::Value& elem) mutable { result.emplace_back(JsonToSnapshot(elem)); });
  return result;
}

SnapshotLog JsonToSnapshotLogEntry(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.IsObject()");
  }

  int64_t timestamp_ms = ExtractInt64Field(document, Names::timestamp_ms);
  int64_t snapshot_id = ExtractInt64Field(document, Names::snapshot_id);

  return {timestamp_ms, snapshot_id};
}

std::optional<std::vector<SnapshotLog>> ExtractSnapshotLog(const rapidjson::Value& document) {
  static constexpr const char* field_name = Names::snapshot_log;
  if (!document.HasMember(field_name)) {
    return std::nullopt;
  }
  std::vector<SnapshotLog> result;
  ProcessArray(document[field_name],
               [&result](const rapidjson::Value& elem) mutable { result.emplace_back(JsonToSnapshotLogEntry(elem)); });
  return result;
}

MetadataLog JsonToMetadataLogEntry(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.IsObject()");
  }

  int64_t timestamp_ms = ExtractInt64Field(document, Names::timestamp_ms);
  std::string metadata_file = ExtractStringField(document, Names::metadata_file);

  return {timestamp_ms, std::move(metadata_file)};
}

std::optional<std::vector<MetadataLog>> ExtractMetadataLog(const rapidjson::Value& document) {
  static constexpr const char* field_name = Names::metadata_log;
  if (!document.HasMember(field_name)) {
    return std::nullopt;
  }
  std::vector<MetadataLog> result;
  ProcessArray(document[field_name],
               [&result](const rapidjson::Value& elem) mutable { result.emplace_back(JsonToMetadataLogEntry(elem)); });
  return result;
}

std::optional<std::map<std::string, std::string>> ExtractProperties(const rapidjson::Value& document) {
  static constexpr const char* field_name = Names::properties;
  if (!document.HasMember(field_name)) {
    return std::nullopt;
  }
  return JsonToStringMap(document[field_name]);
}

SortField JsonToSortField(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.IsObject()");
  }

  // TODO(chertus): lower_case
  auto str_direction = ExtractStringField(document, Names::direction);
  auto str_null_order = ExtractStringField(document, Names::null_order);
  return SortField{
      .transform = ExtractStringField(document, Names::transform),
      .source_id = ExtractInt32Field(document, Names::source_id),
      .direction =
          ((str_direction == Names::asc || str_direction == "ASC") ? SortDirection::kAsc : SortDirection::kDesc),
      .null_order = ((str_null_order == Names::nulls_first) ? NullOrder::kNullsFirst : NullOrder::kNullsLast)};
}

std::shared_ptr<SortOrder> JsonToSortOrders(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.IsObject()");
  }

  int32_t order_id = ExtractInt32Field(document, Names::order_id);
  std::vector<SortField> fields;
  ProcessArray(document[Names::fields],
               [&fields](const rapidjson::Value& elem) mutable { fields.emplace_back(JsonToSortField(elem)); });
  return std::make_shared<SortOrder>(SortOrder{order_id, fields});
}

std::vector<std::shared_ptr<SortOrder>> ExtractSortOrders(const rapidjson::Value& document) {
  static constexpr const char* field_name = Names::sort_orders;
  if (!document.HasMember(field_name)) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.HasMember(" + std::string(field_name) + ")");
  }
  std::vector<std::shared_ptr<SortOrder>> result;
  ProcessArray(document[field_name],
               [&result](const rapidjson::Value& elem) mutable { result.emplace_back(JsonToSortOrders(elem)); });
  return result;
}

SnapshotRef JsonToRef(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.IsObject()");
  }

  return SnapshotRef{.snapshot_id = ExtractInt64Field(document, Names::snapshot_id),
                     .type = ExtractStringField(document, Names::type),
                     .min_snapshots_to_keep = ExtractOptionalInt32Field(document, Names::min_snapshots_to_keep),
                     .max_snapshot_age_ms = ExtractOptionalInt64Field(document, Names::max_snapshot_age_ms),
                     .max_ref_age_ms = ExtractOptionalInt64Field(document, Names::max_ref_age_ms)};
}

std::map<std::string, SnapshotRef> JsonToRefsMap(const rapidjson::Value& document) {
  if (!document.IsObject()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": !document.IsObject()");
  }

  std::map<std::string, SnapshotRef> result;
  for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it) {
    if (!it->name.IsString()) {
      throw std::runtime_error(std::string(__FUNCTION__) + ": !it->name.IsString()");
    }
    result.emplace(it->name.GetString(), JsonToRef(it->value));
  }
  return result;
}

std::optional<std::map<std::string, SnapshotRef>> ExtractRefs(const rapidjson::Value& document) {
  static constexpr const char* field_name = Names::refs;
  if (!document.HasMember(field_name)) {
    return std::nullopt;
  }
  return JsonToRefsMap(document[field_name]);
}

}  // namespace

std::optional<std::string> TableMetadataV2::GetCurrentManifestListPath() const {
  if (!current_snapshot_id.has_value() || snapshots.empty()) {
    return std::nullopt;
  }
  for (const auto& snapshot : snapshots) {
    if (snapshot->snapshot_id == current_snapshot_id.value()) {
      return snapshot->manifest_list_location;
    }
  }
  return std::nullopt;
}

std::shared_ptr<Schema> TableMetadataV2::GetCurrentSchema() const {
  if (!current_snapshot_id.has_value() || snapshots.empty()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": no current snapshot");
  }
  std::optional<int64_t> schema_id;
  for (const auto& snapshot : snapshots) {
    if (snapshot->snapshot_id == current_snapshot_id.value()) {
      if (!snapshot->schema_id.has_value()) {
        throw std::runtime_error(std::string(__FUNCTION__) + ": no schema id");
      }
      schema_id = snapshot->schema_id.value();
    }
  }
  if (!schema_id.has_value()) {
    throw std::runtime_error(std::string(__FUNCTION__) + ": no current snapshot");
  }
  for (const auto& schema : schemas) {
    if (schema->SchemaId() == schema_id.value()) {
      return schema;
    }
  }
  throw std::runtime_error(std::string(__FUNCTION__) + ": no schema with current schema id");
}

std::shared_ptr<SortOrder> TableMetadataV2::GetSortOrder() const {
  for (auto order : sort_orders) {
    if (order->order_id == default_sort_order_id) {
      return order;
    }
  }
  return {};
}

int32_t TableMetadataV2::SetSortOrder(std::shared_ptr<SortOrder> order) {
  std::unordered_set<int32_t> ids;
  for (auto& known_order : sort_orders) {
    if (known_order->fields == order->fields) {
      default_sort_order_id = known_order->order_id;
      return default_sort_order_id;
    }
    ids.emplace(known_order->order_id);
  }
  if (order->order_id == 0 || ids.contains(order->order_id)) {
    order->order_id = 1;
    while (ids.contains(order->order_id)) {
      ++order->order_id;
    }
  }
  sort_orders.push_back(order);
  default_sort_order_id = order->order_id;
  return default_sort_order_id;
}

std::shared_ptr<TableMetadataV2> TableMetadataV2Builder::Build() {
#define ASSERT_HAS_VALUE(field)                                                                            \
  if (!field.has_value()) {                                                                                \
    throw std::runtime_error("TableMetadataV2Builder::Build(): !" + std::string(#field) + ".has_value()"); \
  }

  ASSERT_HAS_VALUE(table_uuid);
  ASSERT_HAS_VALUE(location);
  ASSERT_HAS_VALUE(last_sequence_number);
  ASSERT_HAS_VALUE(last_updated_ms);
  ASSERT_HAS_VALUE(last_column_id);
  ASSERT_HAS_VALUE(schemas);
  ASSERT_HAS_VALUE(current_schema_id);
  ASSERT_HAS_VALUE(partition_specs);
  ASSERT_HAS_VALUE(default_spec_id);
  ASSERT_HAS_VALUE(last_partition_id);
  ASSERT_HAS_VALUE(sort_orders);
  ASSERT_HAS_VALUE(default_sort_order_id);

#undef ASSERT_HAS_VALUE

#define ASSERT_GE(field, value)                                                                          \
  if (field.has_value() && *field < value) {                                                             \
    throw std::runtime_error("TableMetadataV2Builder::Build(): !" + std::string(#field) + " condition"); \
  }

  ASSERT_GE(last_sequence_number, 0);
  ASSERT_GE(last_column_id, 0);
  ASSERT_GE(current_schema_id, 0);
  ASSERT_GE(default_spec_id, 0);
  ASSERT_GE(last_partition_id, 0);
  // ASSERT_GE(current_snapshot_id, 0); // -1 for initial appears
  ASSERT_GE(default_sort_order_id, 0);

#undef ASSERT_GE

  return std::make_shared<TableMetadataV2>(
      std::move(table_uuid.value()), std::move(location.value()), last_sequence_number.value(), last_updated_ms.value(),
      last_column_id.value(), std::move(schemas.value()), current_schema_id.value(), std::move(partition_specs.value()),
      default_spec_id.value(), last_partition_id.value(),
      (properties ? std::move(properties.value()) : std::map<std::string, std::string>{}), current_snapshot_id,
      (snapshots ? std::move(snapshots.value()) : std::vector<std::shared_ptr<Snapshot>>{}),
      (snapshot_log ? std::move(snapshot_log.value()) : std::vector<SnapshotLog>{}),
      (metadata_log ? std::move(metadata_log.value()) : std::vector<MetadataLog>{}),
      (sort_orders ? std::move(sort_orders.value()) : std::vector<std::shared_ptr<SortOrder>>{}),
      default_sort_order_id.value(), (refs ? std::move(refs.value()) : std::map<std::string, SnapshotRef>{}));
}

static std::shared_ptr<TableMetadataV2> MakeTableMetadataV2(const rapidjson::Document& document) {
  TableMetadataV2Builder builder;
  builder.table_uuid = ExtractStringField(document, Names::table_uuid);
  builder.location = ExtractStringField(document, Names::location);
  builder.last_sequence_number = ExtractInt64Field(document, Names::last_sequence_number);
  builder.last_updated_ms = ExtractInt64Field(document, Names::last_updated_ms);
  builder.last_column_id = ExtractInt32Field(document, Names::last_column_id);
  builder.schemas = ExtractSchemas(document);
  builder.current_schema_id = ExtractInt32Field(document, Names::current_schema_id);
  builder.partition_specs = ExtractPartitionSpecs(document);
  builder.default_spec_id = ExtractInt32Field(document, Names::default_spec_id);
  builder.last_partition_id = ExtractInt32Field(document, Names::last_partition_id);
  builder.properties = ExtractProperties(document);
  builder.current_snapshot_id = ExtractOptionalInt64Field(document, Names::current_snapshot_id);
  builder.snapshots = ExtractSnapshots(document);
  builder.snapshot_log = ExtractSnapshotLog(document);
  builder.metadata_log = ExtractMetadataLog(document);
  builder.sort_orders = ExtractSortOrders(document);
  builder.default_sort_order_id = ExtractInt32Field(document, Names::default_sort_order_id);
  builder.refs = ExtractRefs(document);
  return builder.Build();
}

namespace ice_tea {

std::shared_ptr<TableMetadataV2> ReadTableMetadataV2(const std::string& json) {
  rapidjson::Document document;
  document.Parse(json.c_str(), json.size());
  if (!document.IsObject()) {
    return {};
  }

  return MakeTableMetadataV2(document);
}

std::shared_ptr<TableMetadataV2> ReadTableMetadataV2(std::istream& istream) {
  rapidjson::Document document;
  rapidjson::IStreamWrapper isw(istream);
  document.ParseStream(isw);
  if (!document.IsObject()) {
    return {};
  }

  return MakeTableMetadataV2(document);
}

std::string WriteTableMetadataV2(const TableMetadataV2& metadata, bool pretty) {
  rapidjson::Document document;
  document.SetObject();

  WriterContext ctx(document.GetAllocator());

  ctx.WriteIntField(document, Names::format_version, metadata.format_vesion);
  ctx.WriteStringField(document, Names::table_uuid, metadata.table_uuid);
  ctx.WriteStringField(document, Names::location, metadata.location);
  ctx.WriteIntField(document, Names::last_sequence_number, metadata.last_sequence_number);
  ctx.WriteIntField(document, Names::last_updated_ms, metadata.last_updated_ms);
  ctx.WriteIntField(document, Names::last_column_id, metadata.last_column_id);
  ctx.WriteIntField(document, Names::current_schema_id, metadata.current_schema_id);
  ctx.WriteSchemas(document, metadata.schemas);
  ctx.WriteIntField(document, Names::default_spec_id, metadata.default_spec_id);
  ctx.WritePartitionSpec(document, metadata.partition_specs);
  ctx.WriteIntField(document, Names::last_partition_id, metadata.last_partition_id);
  ctx.WriteIntField(document, Names::default_sort_order_id, metadata.default_sort_order_id);
  ctx.WriteSortOrder(document, metadata.sort_orders);
  ctx.WriteProperties(document, metadata.properties);
  if (metadata.current_snapshot_id) {
    ctx.WriteIntField(document, Names::current_snapshot_id, *metadata.current_snapshot_id);
  }
  ctx.WriteRefs(document, metadata.refs);
  ctx.WriteSnapshots(document, metadata.snapshots);
  if (!metadata.snapshot_log.empty()) {
    ctx.WriteSnapshotLog(document, metadata.snapshot_log);
  }
  if (!metadata.metadata_log.empty()) {
    ctx.WriteMetadataLog(document, metadata.metadata_log);
  }

  rapidjson::StringBuffer s;
  if (pretty) {
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);
    writer.SetIndent(' ', 2);
    writer.SetFormatOptions(rapidjson::PrettyFormatOptions::kFormatSingleLineArray);
    document.Accept(writer);
  } else {
    rapidjson::Writer<rapidjson::StringBuffer> writer(s);
    document.Accept(writer);
  }
  return s.GetString();
}

}  // namespace ice_tea
}  // namespace iceberg
