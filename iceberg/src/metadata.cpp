#include "iceberg/src/metadata.h"

#include "rapidjson/document.h"

namespace iceberg {

namespace {
std::string ExtractStringField(const rapidjson::Document& document,
                               const std::string& field_name) {
  const char* c_str = field_name.c_str();
  if (!document.HasMember(c_str)) {
    throw std::runtime_error("MakeMetadata: !document.HasMember(" + field_name +
                             ")");
  }
  if (!document[c_str].IsString()) {
    throw std::runtime_error("MakeMetadata: !document[" + field_name +
                             "].IsString())");
  }
  return std::string(document[c_str].GetString(),
                     document[c_str].GetStringLength());
}
}  // namespace

TableMetadataV2 TableMetadataV2Builder::Build() && {
  if (!table_uuid_.has_value()) {
    throw std::runtime_error(
        "TableMetadataV2Builder::Build(): !table_uuid_.has_value()");
  }
  if (!location_.has_value()) {
    throw std::runtime_error(
        "TableMetadataV2Builder::Build(): !location_.has_value()");
  }
  return TableMetadataV2(std::move(table_uuid_.value()),
                         std::move(location_.value()));
}

TableMetadataV2 MakeMetadata(const std::string& json) {
  TableMetadataV2Builder builder;

  rapidjson::Document document;
  document.Parse(json.c_str(), json.size());
  if (!document.IsObject()) {
    throw std::runtime_error("MakeMetadata: !document.IsObject()");
  }

  builder.SetTableUUID(ExtractStringField(document, "table-uuid"));
  builder.SetLocation(ExtractStringField(document, "location"));

  return std::move(builder).Build();
}

}  // namespace iceberg
