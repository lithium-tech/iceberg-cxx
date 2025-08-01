#include "iceberg/streams/iceberg/schema_name_mapper.h"

namespace iceberg {
namespace {

void Ensure(bool cond, const std::string& msg) {
  if (!cond) {
    throw std::runtime_error(msg);
  }
}

}  // namespace

SchemaNameMapper::SchemaNameMapper(const std::string& json) : doc_(std::make_shared<rapidjson::Document>()) {
  doc_->Parse(json.c_str(), json.size());
  Ensure(!doc_->HasParseError(), "schema.name-mapping.default is not a valid JSON");
  root_mapper_ = std::make_shared<SchemaNameMapper::Mapper>(*doc_);
}

std::shared_ptr<SchemaNameMapper::Mapper> SchemaNameMapper::GetRootMapper() const { return root_mapper_; }

SchemaNameMapper::Mapper::Mapper(const rapidjson::Value& doc) {
  Ensure(doc.IsArray(), std::string(__PRETTY_FUNCTION__) + ": doc is not an Array");
  int size = doc.Size();
  for (int i = 0; i < size; ++i) {
    Ensure(doc[i].IsObject(), std::string(__PRETTY_FUNCTION__) + ": doc elements must be objects");
    Ensure(doc[i].HasMember(names),
           std::string(__PRETTY_FUNCTION__) + ": doc elements must contain" + names + " field");
    const auto& names_field = doc[i][names];
    Ensure(names_field.IsArray(), std::string(__PRETTY_FUNCTION__) + ": " + names + " field must be an array");
    if (doc[i].HasMember(field_id)) {
      Ensure(doc[i][field_id].IsInt(), std::string(__PRETTY_FUNCTION__) + ": " + field_id + " field must be an int");
      int field_id_value = doc[i][field_id].GetInt();
      int names_sz = names_field.Size();
      for (int j = 0; j < names_sz; ++j) {
        Ensure(names_field[j].IsString(),
               std::string(__PRETTY_FUNCTION__) + ": " + names + " field must be an array of strings");
        const bool inserted =
            mp_.insert(std::make_pair(std::string(names_field[j].GetString()), field_id_value)).second;
        Ensure(inserted, std::string(__PRETTY_FUNCTION__) + ": names at the same level are not unique");
      }
    }
  }
}

std::optional<int32_t> SchemaNameMapper::Mapper::GetFieldIdByName(const std::string& name) const {
  auto it = mp_.find(name);
  if (it == mp_.end()) {
    return std::nullopt;
  }
  return it->second;
}

}  // namespace iceberg
