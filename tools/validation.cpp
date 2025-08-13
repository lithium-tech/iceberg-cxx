#include "validation.h"

#include <algorithm>
#include <fstream>

#include "arrow/util/key_value_metadata.h"
#include "iceberg/common/error.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/table_metadata.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"

namespace iceberg::tools {
bool RestrictionsTableMetadata::operator==(const RestrictionsTableMetadata& other) const = default;

RestrictionsTableMetadata::RestrictionsTableMetadata() {}

RestrictionsTableMetadata::RestrictionsTableMetadata(
    int32_t max_array_dimensionality_, std::unordered_set<std::string>&& partitioned_,
    std::unordered_set<std::string>&& sorted, std::vector<std::string>&& required_tags_,
    std::vector<std::pair<std::string, std::optional<std::string>>>&& properties_,
    std::unordered_set<iceberg::TypeID>&& forbidden_types_)
    : max_array_dimensionality(max_array_dimensionality_),
      partitioned(std::move(partitioned_)),
      sorted(std::move(sorted)),
      required_tags(std::move(required_tags_)),
      properties(std::move(properties_)),
      forbidden_types(std::move(forbidden_types_)) {}

bool RestrictionsManifests::operator==(const RestrictionsManifests& other) const = default;

RestrictionsManifests::RestrictionsManifests() {}

RestrictionsManifests::RestrictionsManifests(int32_t max_equality_deletes_count_, int32_t max_position_deletes_count_,
                                             int32_t max_data_files_count_, int32_t max_delete_files_count_,
                                             int32_t max_row_groups_per_file_, int32_t max_row_group_size_percentile_,
                                             int32_t min_row_group_size_percentile_, int64_t max_row_group_size_bytes_,
                                             int64_t min_row_group_size_bytes_)
    : max_equality_deletes_count(max_equality_deletes_count_),
      max_position_deletes_count(max_position_deletes_count_),
      max_data_files_count(max_data_files_count_),
      max_delete_files_count(max_delete_files_count_),
      max_row_groups_per_file(max_row_groups_per_file_),
      max_row_group_size_percentile(max_row_group_size_percentile_),
      min_row_group_size_percentile(min_row_group_size_percentile_),
      max_row_group_size_bytes(max_row_group_size_bytes_),
      min_row_group_size_bytes(min_row_group_size_bytes_) {}

namespace {
class ErrorLog {
 public:
  void Add(const std::string& message) { errors_ += message; }

  bool Check(bool condition, const std::string& message) {
    if (!condition) {
      Add(message);
      return false;
    }
    return true;
  }

  // use this function if you check condition more than once, but don't want to see the same message many times
  bool CheckOnce(bool condition, const std::string& message) {
    if (!condition && !was_.contains(message)) {
      errors_ += message;
      was_.insert(message);
      return false;
    }
    return true;
  }

  void ThrowIfWasError() { Ensure(errors_.empty(), error_message + errors_); }

 private:
  static constexpr char error_message[] = "The following restrictions have been violated:\n";
  std::string errors_;
  std::unordered_set<std::string> was_;
};

}  // namespace

void IcebergMetadataValidator::ValidateTableMetadata(const iceberg::TableMetadataV2& table_metadata) const {
  Ensure(IsRestrictionsTableMetadataAssigned(), "Restrictions Table metadata are not assigned yet\n");
  ErrorLog error_log;
  auto restrictions = restrictions_table_metadata_.value();
  error_log.Check(
      !table_metadata.partition_specs.empty() || !restrictions.partitioned.contains(table_metadata.table_uuid),
      "No partition spec\n");
  for (auto& i : restrictions.properties) {
    auto iter = table_metadata.properties.find(i.first);
    if (error_log.Check(iter != table_metadata.properties.end(), "Missing required properties: " + i.first + '\n') &&
        i.second.has_value()) {
      error_log.Check(iter->second == i.second.value(), "Wrong required properties: " + i.second.value() + '\n');
    }
  }
  error_log.Check(!!table_metadata.GetSortOrder() || !restrictions.sorted.contains(table_metadata.table_uuid),
                  "No sort order\n");
  for (const auto schema : table_metadata.schemas) {
    for (const auto& column : schema->Columns()) {
      std::shared_ptr<const iceberg::types::Type> cur_type = column.type;
      error_log.CheckOnce(!restrictions.forbidden_types.contains(cur_type->TypeId()),
                          "Forbidden type: " + types::PrimitiveTypeToName(cur_type->TypeId()).value() + '\n');
      for (int32_t dim = 0; dim < restrictions.max_array_dimensionality && cur_type->IsListType(); ++dim) {
        cur_type = std::dynamic_pointer_cast<const iceberg::types::ListType>(cur_type)->ElementType();
      }
      error_log.CheckOnce(
          !cur_type->IsListType(),
          "Array must contain no more than " + std::to_string(restrictions.max_array_dimensionality) + " dimensions\n");
    }
  }
  for (const auto& name : restrictions.required_tags) {
    error_log.Check(table_metadata.refs.contains(name), "Required tag/branch is missed: " + name + '\n');
  }
  error_log.ThrowIfWasError();
}

void IcebergMetadataValidator::ValidateTableMetadata(std::shared_ptr<iceberg::TableMetadataV2> table_metadata) const {
  ValidateTableMetadata(*table_metadata);
}

void IcebergMetadataValidator::ValidateManifests(const std::vector<Manifest>& manifests) const {
  Ensure(IsRestrictionsManifestsAssigned(), "Restrictions manifests are not assigned yet\n");
  ErrorLog error_log;
  auto restrictions = restrictions_manifests_.value();
  int32_t equality_deletes_count = 0;
  int32_t position_deletes_count = 0;
  int32_t data_files_count = 0;
  int32_t delete_files_count = 0;
  for (const auto& manifest : manifests) {
    for (const auto& manifest_entry : manifest.entries) {
      auto helper = ManifestEntryHelper(manifest_entry);
      if (helper.IsEqualityDeletes()) {
        ++equality_deletes_count;
        ++delete_files_count;
      }
      if (helper.IsPositionDeletes()) {
        ++position_deletes_count;
        ++delete_files_count;
      }
      if (helper.IsData()) {
        ++data_files_count;
      }
      error_log.CheckOnce(manifest_entry.data_file.file_format == "PARQUET", "Unsupported file format\n");
      size_t num_row_groups = manifest_entry.data_file.split_offsets.size();
      error_log.CheckOnce(num_row_groups <= restrictions.max_row_groups_per_file, "Too many row groups\n");
      std::vector<int64_t> row_group_sizes(num_row_groups);
      for (size_t i = 0; i + 1 < num_row_groups; i++) {
        row_group_sizes[i] = manifest_entry.data_file.split_offsets[i + 1] - manifest_entry.data_file.split_offsets[i];
      }
      row_group_sizes.back() =
          manifest_entry.data_file.file_size_in_bytes -
          (manifest_entry.data_file.split_offsets.back() - manifest_entry.data_file.split_offsets[0]);
      std::sort(row_group_sizes.begin(), row_group_sizes.end());
      error_log.CheckOnce(row_group_sizes[restrictions.min_row_group_size_percentile * (num_row_groups - 1) / 100] >=
                              restrictions.min_row_group_size_bytes,
                          "row group size is too small\n");
      error_log.CheckOnce(row_group_sizes[restrictions.max_row_group_size_percentile * (num_row_groups - 1) / 100] <=
                              restrictions.max_row_group_size_bytes,
                          "row group size is too big\n");
    }
  }

  error_log.Check(equality_deletes_count <= restrictions.max_equality_deletes_count,
                  "Too many equality deletes : expected no more than " +
                      std::to_string(restrictions.max_equality_deletes_count) + ", actual is " +
                      std::to_string(equality_deletes_count) + '\n');
  error_log.Check(position_deletes_count <= restrictions.max_position_deletes_count,
                  "Too many position deletes : expected no more than " +
                      std::to_string(restrictions.max_position_deletes_count) + ", actual is " +
                      std::to_string(position_deletes_count) + '\n');
  error_log.Check(delete_files_count <= restrictions.max_delete_files_count,
                  "Too many delete files : expected no more than " +
                      std::to_string(restrictions.max_delete_files_count) + ", actual is " +
                      std::to_string(delete_files_count) + '\n');
  error_log.Check(data_files_count <= restrictions.max_data_files_count,
                  "Too many data files : expected no more than " + std::to_string(restrictions.max_data_files_count) +
                      ", actual is " + std::to_string(data_files_count) + '\n');
  error_log.ThrowIfWasError();
}

void IcebergMetadataValidator::SetRestrictionsTableMetadata(const RestrictionsTableMetadata& restrictions) {
  restrictions_table_metadata_ = restrictions;
}

void IcebergMetadataValidator::SetRestrictionsTableMetadata(RestrictionsTableMetadata&& restrictions) {
  restrictions_table_metadata_ = std::move(restrictions);
}

void IcebergMetadataValidator::SetRestrictionsManifests(const RestrictionsManifests& restrictions) {
  restrictions_manifests_ = restrictions;
}

void IcebergMetadataValidator::SetRestrictionsManifests(RestrictionsManifests&& restrictions) {
  restrictions_manifests_ = std::move(restrictions);
}

bool IcebergMetadataValidator::IsRestrictionsManifestsAssigned() const { return restrictions_manifests_.has_value(); }

bool IcebergMetadataValidator::IsRestrictionsTableMetadataAssigned() const {
  return restrictions_table_metadata_.has_value();
}

RestrictionsTableMetadata& IcebergMetadataValidator::GetRestrictionsTableMetadata() {
  Ensure(IsRestrictionsTableMetadataAssigned(), "Restrictions Table metadata are not assigned yet\n");
  return restrictions_table_metadata_.value();
}

const RestrictionsTableMetadata& IcebergMetadataValidator::GetRestrictionsTableMetadata() const {
  Ensure(IsRestrictionsTableMetadataAssigned(), "Restrictions Table metadata are not assigned yet\n");
  return restrictions_table_metadata_.value();
}

RestrictionsManifests& IcebergMetadataValidator::GetRestrictionsManifests() {
  Ensure(IsRestrictionsManifestsAssigned(), "Restrictions Manifests are not assigned yet\n");
  return restrictions_manifests_.value();
}

const RestrictionsManifests& IcebergMetadataValidator::GetRestrictionsManifests() const {
  Ensure(IsRestrictionsManifestsAssigned(), "Restrictions Manifests are not assigned yet\n");
  return restrictions_manifests_.value();
}

static void GetIntArg(const rapidjson::Document& doc, int32_t& arg, ErrorLog& error_log, const std::string& name) {
  auto iter = doc.FindMember(name.c_str());
  if (iter != doc.MemberEnd()) {
    Ensure(iter->value.IsInt(), "Wrong argument type: " + name + '\n');
    arg = iter->value.GetInt();
  } else {
    error_log.Add("Argument is missed: " + name + '\n');
  }
}

static void GetInt64Arg(const rapidjson::Document& doc, int64_t& arg, ErrorLog& error_log, const std::string& name) {
  auto iter = doc.FindMember(name.c_str());
  if (iter != doc.MemberEnd()) {
    Ensure(iter->value.IsInt64(), "Wrong argument type: " + name + '\n');
    arg = iter->value.GetInt64();
  } else {
    error_log.Add("Argument is missed: " + name + '\n');
  }
}

RestrictionsTableMetadata RestrictionsTableMetadata::Read(std::istream& istream, bool expect_all_params) {
  rapidjson::Document doc;
  rapidjson::IStreamWrapper isw(istream);
  doc.ParseStream(isw);
  Ensure(doc.IsObject(), "Invalid JSON\n");
  RestrictionsTableMetadata result{};
  ErrorLog error_log;
  GetIntArg(doc, result.max_array_dimensionality, error_log, "max_array_dimensionality");

  auto iter = doc.FindMember("partitioned");
  if (iter != doc.MemberEnd()) {
    Ensure(iter->value.IsArray(), "Wrong argument type: partitioned\n");
    for (const auto& elem : iter->value.GetArray()) {
      Ensure(elem.IsString(), "Wrong argument type: partitioned\n");
      result.partitioned.insert(elem.GetString());
    }
  } else {
    error_log.Add("Argument is missed: partitioned\n");
  }

  iter = doc.FindMember("sorted");
  if (iter != doc.MemberEnd()) {
    Ensure(iter->value.IsArray(), "Wrong argument type: sorted\n");
    for (const auto& elem : iter->value.GetArray()) {
      Ensure(elem.IsString(), "Wrong argument type: sorted\n");
      result.sorted.insert(elem.GetString());
    }
  } else {
    error_log.Add("Argument sorted is missed\n");
  }

  iter = doc.FindMember("required_tags");
  if (iter != doc.MemberEnd()) {
    Ensure(iter->value.IsArray(), "Wrong argument type: required_tags\n");
    for (const auto& elem : iter->value.GetArray()) {
      Ensure(elem.IsString(), "Wrong argument type: required_tags\n");
      result.required_tags.push_back(elem.GetString());
    }
  } else {
    error_log.Add("Argument is missed: required_tags\n");
  }

  iter = doc.FindMember("properties");
  if (iter != doc.MemberEnd()) {
    Ensure(iter->value.IsObject(), "Wrong argument type: properties\n");
    for (auto& elem : iter->value.GetObject()) {
      Ensure(elem.value.IsString(), "Wrong argument type: properties\n");
      if (elem.value.GetStringLength() == 0) {
        result.properties.emplace_back(elem.name.GetString(), std::nullopt);
      } else {
        result.properties.emplace_back(elem.name.GetString(), elem.value.GetString());
      }
    }
  } else {
    error_log.Add("Argument is missed: properties\n");
  }

  iter = doc.FindMember("forbidden_types");
  if (iter != doc.MemberEnd()) {
    Ensure(iter->value.IsArray(), "Wrong argument type: forbidden_types\n");
    for (const auto& elem : iter->value.GetArray()) {
      Ensure(elem.IsString(), "Wrong argument type: forbidden_types\n");
      auto type = types::NameToPrimitiveType(elem.GetString());
      Ensure(type.has_value(),
             "In forbidden_types: Unknown type: " + static_cast<std::string>(elem.GetString()) + '\n');
      result.forbidden_types.insert(type.value());
    }
  } else {
    error_log.Add("Argument is missed: forbidden_types\n");
  }
  if (expect_all_params) {
    error_log.ThrowIfWasError();
  }
  return result;
}

RestrictionsTableMetadata RestrictionsTableMetadata::Read(const std::string& config_path, bool expect_all_params) {
  std::ifstream ifstream(config_path);
  Ensure(ifstream.is_open(), "Can't open config file\n");
  return Read(ifstream, expect_all_params);
}

RestrictionsManifests RestrictionsManifests::Read(std::istream& istream, bool expect_all_params) {
  rapidjson::Document doc;
  rapidjson::IStreamWrapper isw(istream);
  doc.ParseStream(isw);
  Ensure(doc.IsObject(), "Invalid JSON\n");
  RestrictionsManifests result{};
  ErrorLog error_log;
  GetIntArg(doc, result.max_equality_deletes_count, error_log, "max_equality_deletes_count");
  GetIntArg(doc, result.max_position_deletes_count, error_log, "max_position_deletes_count");
  GetIntArg(doc, result.max_data_files_count, error_log, "max_data_files_count");
  GetIntArg(doc, result.max_delete_files_count, error_log, "max_delete_files_count");
  GetIntArg(doc, result.max_row_groups_per_file, error_log, "max_row_groups_per_file");
  GetIntArg(doc, result.max_row_group_size_percentile, error_log, "max_row_group_size_percentile");
  GetIntArg(doc, result.min_row_group_size_percentile, error_log, "min_row_group_size_percentile");
  GetInt64Arg(doc, result.max_row_group_size_bytes, error_log, "max_row_group_size_bytes");
  GetInt64Arg(doc, result.min_row_group_size_bytes, error_log, "min_row_group_size_bytes");
  if (expect_all_params) {
    error_log.ThrowIfWasError();
  }
  return result;
}

RestrictionsManifests RestrictionsManifests::Read(const std::string& config_path, bool expect_all_params) {
  std::ifstream ifstream(config_path);
  Ensure(ifstream.is_open(), "Can't open config file\n");
  return Read(ifstream, expect_all_params);
}
}  // namespace iceberg::tools
