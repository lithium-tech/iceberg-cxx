#pragma once

#include <map>
#include <optional>
#include <string>

#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace iceberg::json_parse {

std::string ExtractStringField(const rapidjson::Value& document, const std::string& field_name);

int64_t ExtractInt64Field(const rapidjson::Value& document, const std::string& field_name);

std::optional<int32_t> ExtractOptionalInt32Field(const rapidjson::Value& document, const std::string& field_name);

std::optional<int64_t> ExtractOptionalInt64Field(const rapidjson::Value& document, const std::string& field_name);

int32_t ExtractInt32Field(const rapidjson::Value& document, const std::string& field_name);

bool ExtractBooleanField(const rapidjson::Value& document, const std::string& field_name);

std::map<std::string, std::string> JsonToStringMap(const rapidjson::Value& document);

std::map<std::string, std::string> ExtractStringMap(const rapidjson::Value& document, const std::string& field_name);

}  // namespace iceberg::json_parse
