#pragma once

#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "metadata_tree.h"

namespace iceberg::tools {
struct Restrictions {
  int32_t max_equality_deletes_count = INT32_MAX;
  int32_t max_position_deletes_count = INT32_MAX;
  int32_t max_data_files_count = INT32_MAX;
  int32_t max_delete_files_count = INT32_MAX;
  int32_t max_row_groups_per_file = INT32_MAX;
  int32_t max_array_dimensionality = INT32_MAX;
  // int32_t max_layers_per_snapshot;
  int32_t max_row_group_size_percentile = 100;
  int32_t min_row_group_size_percentile = 0;
  int64_t max_row_group_size_limitation = INT64_MAX;
  int64_t min_row_group_size_limitation = INT64_MIN;
  int64_t max_metadata_size_per_table = INT64_MAX;

  std::unordered_set<std::string> partitioned;  // table uuids
  std::unordered_set<std::string> sorted;       // table uuids
  std::vector<std::string> required_tags;
  // std::vector<std::string> required_fields;
  std::vector<std::string> required_statistics;
  std::vector<std::pair<std::string, std::optional<std::string>>> properties;
  std::unordered_set<iceberg::TypeID> forbidden_types;  // ReadRestrictions convert string names to TypeID

  Restrictions();  // don't forget to fill fields with your values
  Restrictions(int32_t max_equality_deletes_count_, int32_t max_position_deletes_count_, int32_t max_data_files_count_,
               int32_t max_delete_files_count_, int32_t max_row_groups_per_file_, int32_t max_array_dimensionality_,
               int32_t max_row_group_size_percentile_, int32_t min_row_group_size_percentile_,
               int64_t max_row_group_size_limitation_, int64_t min_row_group_size_limitation_,
               int64_t max_metadata_size_per_table_, std::unordered_set<std::string>&& partitioned_,
               std::unordered_set<std::string>&& sorted, std::vector<std::string>&& required_tags_,
               std::vector<std::string>&& required_statistics_,
               std::vector<std::pair<std::string, std::optional<std::string>>>&& properties_,
               std::unordered_set<iceberg::TypeID>&& forbidden_types_);
  bool operator==(const Restrictions& other) const;
};

Restrictions ReadRestrictions(std::istream& istream);
Restrictions ReadRestrictions(const std::string& config_path);

class Validator {
 public:
  Validator(const Restrictions& restrictions);

  // checks properties, partition_specs, sort_order, types, array dimensionality, tags
  void ValidateTableMetadata(std::shared_ptr<iceberg::TableMetadataV2> table_metadata) const;

  // checks amount of data files, delete files (both types), file format, amount of row groups per file, sizes of row
  // groups
  void ValidateManifests(const std::vector<std::shared_ptr<iceberg::Manifest>>& manifests) const;

  Restrictions& GetRestrictions();
  const Restrictions& GetRestrictions() const;

 private:
  Restrictions restrictions_;
};
}  // namespace iceberg::tools
