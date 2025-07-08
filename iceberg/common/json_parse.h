#pragma once

#include <map>
#include <optional>
#include <string>

#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace iceberg::json_parse {

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

}  // namespace iceberg::json_parse
