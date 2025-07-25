#pragma once

#include <unordered_set>

#include "iceberg/common/json_parse.h"

namespace iceberg {

template <typename T>
class UniqueSet {
 public:
  void Insert(const T& t) {
    if (!set_.insert(t).second) {
      throw std::runtime_error(std::string(__FUNCTION__) + ": duplicate element");
    }
  }

 private:
  std::unordered_set<T> set_;
};

class SchemaNameMapper {
 public:
  class Node {
   public:
    Node(const rapidjson::Value& doc) : doc_(doc) {
      if (!doc_.IsArray()) {
        throw std::runtime_error(std::string(__FUNCTION__) + ": !doc.IsArray()");
      }
    }

    // call Validate() first, if you are not sure that schema_name_mapping is valid
    std::optional<int32_t> GetFieldIdByName(const std::string& name) const {
      const size_t sz = doc_.Size();
      for (size_t i = 0; i < sz; ++i) {
        const rapidjson::Value& names_array = doc_[i][names];
        const size_t names_sz = names_array.Size();
        for (size_t j = 0; j < names_sz; ++j) {
          if (names_array[j].GetString() == name) {
            return json_parse::ExtractOptionalInt32Field(doc_[i], field_id);
          }
        }
      }
      return std::nullopt;
    }

    // Checks that the structure is correct and all names at this level are unique
    void Validate() const {
      const size_t sz = doc_.Size();
      UniqueSet<std::string> all_names;
      for (size_t i = 0; i < sz; ++i) {
        if (!doc_[i].HasMember(names)) {
          throw std::runtime_error(std::string(__FUNCTION__) +
                                   ": \"names\" is a required field in schema.name-mapping.default");
        }
        const rapidjson::Value& names_array = doc_[i][names];
        if (!names_array.IsArray()) {
          throw std::runtime_error(std::string(__FUNCTION__) + ": \"names\" is not an array");
        }
        const size_t names_sz = names_array.Size();
        for (size_t j = 0; j < names_sz; ++j) {
          if (!names_array[j].IsString()) {
            throw std::runtime_error(std::string(__FUNCTION__) + ": \"names\" is not an array of strings");
          }
          all_names.Insert(names_array[j].GetString());
        }
      }
    }

   private:
    const rapidjson::Value& doc_;
  };

  SchemaNameMapper(const std::string& json) {
    doc_.Parse(json.c_str(), json.size());
    if (doc_.HasParseError()) {
      throw std::runtime_error("schema.name-mapping.default is not a valid JSON");
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
