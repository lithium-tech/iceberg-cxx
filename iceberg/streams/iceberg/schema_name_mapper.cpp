#include "iceberg/streams/iceberg/schema_name_mapper.h"

namespace iceberg {
namespace {

void Ensure(bool cond, const std::string& msg) {
  if (!cond) {
    throw std::runtime_error(msg);
  }
}

void InsertOrFail(std::unordered_set<std::string>& set, const std::string& elem) {
  Ensure(!set.contains(elem), "Names at the same level are not unique");
  set.insert(elem);
}

}  // namespace

SchemaNameMapper::SchemaNameMapper(const std::string& json) {
  doc_.Parse(json.c_str(), json.size());
  Ensure(!doc_.HasParseError(), "schema.name-mapping.default is not a valid JSON");
}

SchemaNameMapper::Node SchemaNameMapper::GetRootNode() const { return Node(&doc_); }

SchemaNameMapper::Node::Node(const rapidjson::Value* doc) : doc_(doc) { Validate(); }

std::optional<int32_t> SchemaNameMapper::Node::GetFieldIdByName(const std::string& name) const {
  const auto& doc = *doc_;
  const size_t sz = doc.Size();
  for (size_t i = 0; i < sz; ++i) {
    const rapidjson::Value& names_array = doc[i][names];
    const size_t names_sz = names_array.Size();
    for (size_t j = 0; j < names_sz; ++j) {
      if (names_array[j].GetString() == name) {
        return json_parse::ExtractOptionalInt32Field(doc[i], field_id);
      }
    }
  }
  return std::nullopt;
}

void SchemaNameMapper::Node::Validate() const {
  const auto& doc = *doc_;
  Ensure(doc.IsArray(), std::string(__FUNCTION__) + ": !doc.IsArray()");

  const size_t sz = doc.Size();
  std::unordered_set<std::string> all_names;
  for (size_t i = 0; i < sz; ++i) {
    Ensure(doc[i].HasMember(names),
           std::string(__FUNCTION__) + ": \"names\" is a required field in schema.name-mapping.default");
    const rapidjson::Value& names_array = doc[i][names];
    Ensure(names_array.IsArray(), std::string(__FUNCTION__) + ": \"names\" is not an array");
    const size_t names_sz = names_array.Size();
    for (size_t j = 0; j < names_sz; ++j) {
      Ensure(names_array[j].IsString(), std::string(__FUNCTION__) + ": \"names\" is not an array of strings");
      InsertOrFail(all_names, names_array[j].GetString());
    }
  }
}

}  // namespace iceberg
