#pragma once

#include "iceberg/common/json_parse.h"

namespace iceberg {
class SchemaNameMapper {
 public:
  class Node {
   public:
    Node(const rapidjson::Value& doc) : doc_(doc) {}

    std::optional<int32_t> GetFieldIdByName(const std::string& name) const {
      size_t sz = doc_.Size(); // automatically checks that doc_ is an array
      for (size_t i = 0; i < sz; ++i) {
        if (!doc_[i].HasMember(names)) {
            throw std::runtime_error("");
        }
        if (!doc_[i][names].IsArray()) {

        }
        
      }
    }

    void Validate() const {

    }

   private:
    const rapidjson::Value& doc_;
  };

  SchemaNameMapper(const std::string& json) {
    doc_.Parse(json.c_str(), json.size());
    if (!doc_.IsObject()) {
      throw std::runtime_error("schema.name-mapping.default is not a JSON object");
    }
  }

  Node GetRootNode() const { return Node(doc_); }

  static constexpr const char* names = "names";
  static constexpr const char* field_id = "field-id";
  static constexpr const char* fields = "fields";

 private:
  rapidjson::Document doc_;
};

}  // namespace iceberg
