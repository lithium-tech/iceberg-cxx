#pragma once

#include <memory>
#include <unordered_set>

#include "iceberg/common/json_parse.h"

namespace iceberg {

class SchemaNameMapper {
 public:
  class Mapper {
   public:
    explicit Mapper(std::shared_ptr<rapidjson::Document> main_object, const rapidjson::Value* doc);

    std::optional<int32_t> GetFieldIdByName(const std::string& name) const;

    // TODO: support GetChildMapper(const std::string& name)

    // Checks that the structure is correct and all names at this level are unique
    void Validate() const;

   private:
    // keep shared_ptr to ensure doc_ pointer is correct
    std::shared_ptr<rapidjson::Document> main_object_;
    const rapidjson::Value* doc_;

    // std::set<std::string> validated_child_nodes_;
  };

  SchemaNameMapper(const std::string& json);

  Mapper GetRootMapper() const;

  static constexpr const char* names = "names";
  static constexpr const char* field_id = "field-id";
  static constexpr const char* fields = "fields";

 private:
  std::shared_ptr<rapidjson::Document> doc_;
  mutable bool root_node_validated_ = false;
};

}  // namespace iceberg
