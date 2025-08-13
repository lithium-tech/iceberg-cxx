#include "iceberg/common/json_parse.h"

#include "iceberg/common/error.h"

namespace iceberg::json_parse {

std::string ExtractStringField(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  Ensure(document.HasMember(c_str), std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  Ensure(document[c_str].IsString(), std::string(__FUNCTION__) + ": !document[" + field_name + "].IsString())");

  return std::string(document[c_str].GetString(), document[c_str].GetStringLength());
}

int64_t ExtractInt64Field(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  Ensure(document.HasMember(c_str), std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  Ensure(document[c_str].IsInt64(), std::string(__FUNCTION__) + ": !document[" + field_name + "].IsString())");

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
  Ensure(document.HasMember(c_str), std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  Ensure(document[c_str].IsInt(), std::string(__FUNCTION__) + ": !document[" + field_name + "].IsString())");

  return document[c_str].GetInt();
}

bool ExtractBooleanField(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  Ensure(document.HasMember(c_str), std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");
  Ensure(document[c_str].IsBool(), std::string(__FUNCTION__) + ": !document[" + field_name + "].IsString())");

  return document[c_str].GetBool();
}

std::map<std::string, std::string> JsonToStringMap(const rapidjson::Value& document) {
  std::map<std::string, std::string> result;
  Ensure(document.IsObject(), std::string(__FUNCTION__) + ": !document.IsObject()");

  for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it) {
    Ensure(it->name.IsString(), std::string(__FUNCTION__) + ": !it->name.IsString()");
    Ensure(it->value.IsString(), std::string(__FUNCTION__) + ": !it->value.IsString()");

    result.emplace(it->name.GetString(), it->value.GetString());
  }
  return result;
}

std::map<std::string, std::string> ExtractStringMap(const rapidjson::Value& document, const std::string& field_name) {
  const char* c_str = field_name.c_str();
  Ensure(document.HasMember(c_str), std::string(__FUNCTION__) + ": !document.HasMember(" + field_name + ")");

  return JsonToStringMap(document[c_str]);
}

}  // namespace iceberg::json_parse
