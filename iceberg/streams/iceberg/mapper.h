#pragma once

#include <map>
#include <set>
#include <string>
#include <utility>

namespace iceberg {

class FieldIdMapper {
 public:
  explicit FieldIdMapper(std::map<int, std::string> known_field_id_to_name)
      : known_field_id_to_name_(std::move(known_field_id_to_name)), used_names_([&]() {
          std::set<std::string_view> used;
          for (const auto& [id, name] : known_field_id_to_name_) {
            used.insert(name);
          }
          return used;
        }()) {}

  std::string FieldIdToColumnName(int field_id) const {
    if (known_field_id_to_name_.contains(field_id)) {
      return known_field_id_to_name_.at(field_id);
    }

    return GenerateNameForUnnamedColumn(field_id);
  }

 private:
  std::string GenerateNameForUnnamedColumn(int field_id) const {
    uint64_t c = 0;
    while (true) {
      std::string result = "__unnamed_c" + std::to_string(c) + "_f" + std::to_string(field_id);
      if (used_names_.contains(result)) {
        ++c;
        continue;
      }
      return result;
    }
  }

  const std::map<int, std::string> known_field_id_to_name_;
  const std::set<std::string_view> used_names_;
};

}  // namespace iceberg
