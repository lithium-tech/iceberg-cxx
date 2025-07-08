#pragma once

#include <memory>
#include <unordered_map>

#include "iceberg/common/json_parse.h"

namespace iceberg {

class SchemaNameMapper {
 public:
  class Mapper {
   public:
    explicit Mapper(const rapidjson::Value& doc);

    std::optional<int32_t> GetFieldIdByName(const std::string& name) const;

    // TODO: support GetChildMapper(const std::string& name)
   private:
    std::unordered_map<std::string, int32_t> mp_;
  };

  SchemaNameMapper(const std::string& json);

  std::shared_ptr<Mapper> GetRootMapper() const;

  static constexpr const char* names = "names";
  static constexpr const char* field_id = "field-id";
  static constexpr const char* fields = "fields";

 private:
  std::shared_ptr<rapidjson::Document> doc_;
  std::shared_ptr<Mapper> root_mapper_;
};

}  // namespace iceberg
