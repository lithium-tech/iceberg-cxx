#pragma once

#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "metadata_tree.h"

namespace iceberg::tools {
struct RestrictionsTableMetadata {
  int32_t max_array_dimensionality = INT32_MAX;

  std::unordered_set<std::string> partitioned;  // table uuids
  std::unordered_set<std::string> sorted;       // table uuids
  std::vector<std::string> required_tags;
  std::vector<std::pair<std::string, std::optional<std::string>>> properties;
  std::unordered_set<iceberg::TypeID> forbidden_types;  // ReadRestrictions convert string names to TypeID

  RestrictionsTableMetadata();
  RestrictionsTableMetadata(int32_t max_array_dimensionality_, std::unordered_set<std::string>&& partitioned_,
                            std::unordered_set<std::string>&& sorted, std::vector<std::string>&& required_tags_,
                            std::vector<std::pair<std::string, std::optional<std::string>>>&& properties_,
                            std::unordered_set<iceberg::TypeID>&& forbidden_types_);
  bool operator==(const RestrictionsTableMetadata& other) const;

  static RestrictionsTableMetadata Read(std::istream& istream, bool expect_all_params = false);
  static RestrictionsTableMetadata Read(const std::string& config_path, bool expect_all_params = false);
};

struct RestrictionsManifests {
  int32_t max_equality_deletes_count = INT32_MAX;
  int32_t max_position_deletes_count = INT32_MAX;
  int32_t max_data_files_count = INT32_MAX;
  int32_t max_delete_files_count = INT32_MAX;
  int32_t max_row_groups_per_file = INT32_MAX;
  int32_t max_row_group_size_percentile = 100;
  int32_t min_row_group_size_percentile = 0;
  int64_t max_row_group_size_bytes = INT64_MAX;
  int64_t min_row_group_size_bytes = INT64_MIN;

  RestrictionsManifests();
  RestrictionsManifests(int32_t max_equality_deletes_count_, int32_t max_position_deletes_count_,
                        int32_t max_data_files_count_, int32_t max_delete_files_count_,
                        int32_t max_row_groups_per_file_, int32_t max_row_group_size_percentile_,
                        int32_t min_row_group_size_percentile_, int64_t max_row_group_size_bytes_,
                        int64_t min_row_group_size_bytes_);
  bool operator==(const RestrictionsManifests& other) const;

  static RestrictionsManifests Read(std::istream& istream, bool expect_all_params = false);
  static RestrictionsManifests Read(const std::string& config_path, bool expect_all_params = false);
};

class IcebergMetadataValidator {
 public:
  void SetRestrictionsTableMetadata(const RestrictionsTableMetadata& restrictions);
  void SetRestrictionsTableMetadata(RestrictionsTableMetadata&& restrictions);

  void SetRestrictionsManifests(const RestrictionsManifests& restrictions);
  void SetRestrictionsManifests(RestrictionsManifests&& restrictions);

  // checks properties, partition_specs, sort_order, types, array dimensionality, tags
  void ValidateTableMetadata(std::shared_ptr<iceberg::TableMetadataV2> table_metadata) const;
  void ValidateTableMetadata(const iceberg::TableMetadataV2& table_metadata) const;

  // checks amount of data files, delete files (both types), file format, amount of row groups per file, sizes of row
  // groups
  void ValidateManifests(const std::vector<iceberg::Manifest>& manifests) const;

  bool IsRestrictionsTableMetadataAssigned() const;
  bool IsRestrictionsManifestsAssigned() const;

  RestrictionsTableMetadata& GetRestrictionsTableMetadata();              // throws if no value
  const RestrictionsTableMetadata& GetRestrictionsTableMetadata() const;  // throws if no value

  RestrictionsManifests& GetRestrictionsManifests();              // throws if no value
  const RestrictionsManifests& GetRestrictionsManifests() const;  // throws if no value

 private:
  std::optional<RestrictionsTableMetadata> restrictions_table_metadata_;
  std::optional<RestrictionsManifests> restrictions_manifests_;
};
}  // namespace iceberg::tools
