#pragma once

#include <unordered_set>

#include "iceberg/common/json_parse.h"

namespace iceberg {

class SchemaNameMapper {
 public:
  class Node {
   public:
    explicit Node(const rapidjson::Value* doc);

    std::optional<int32_t> GetFieldIdByName(const std::string& name) const;

    // TODO: support GetChildNode(const std::string& name)

   private:
    // Checks that the structure is correct and all names at this level are unique
    void Validate() const;

    const rapidjson::Value* doc_;
  };

  SchemaNameMapper(const std::string& json);

  Node GetRootNode() const;

  static constexpr const char* names = "names";
  static constexpr const char* field_id = "field-id";
  static constexpr const char* fields = "fields";

 private:
  rapidjson::Document doc_;
};

}  // namespace iceberg
