#include "validation.h"

#include <algorithm>
#include <fstream>

#include "arrow/util/key_value_metadata.h"
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
                                             int32_t min_row_group_size_percentile_,
                                             int64_t max_row_group_size_limitation_,
                                             int64_t min_row_group_size_limitation_,
                                             int64_t max_metadata_size_per_table_)
    : max_equality_deletes_count(max_equality_deletes_count_),
      max_position_deletes_count(max_position_deletes_count_),
      max_data_files_count(max_data_files_count_),
      max_delete_files_count(max_delete_files_count_),
      max_row_groups_per_file(max_row_groups_per_file_),
      max_row_group_size_percentile(max_row_group_size_percentile_),
      min_row_group_size_percentile(min_row_group_size_percentile_),
      max_row_group_size_limitation(max_row_group_size_limitation_),
      min_row_group_size_limitation(min_row_group_size_limitation_),
      max_metadata_size_per_table(max_metadata_size_per_table_) {}

static void Ensure(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void Validator::ValidateTableMetadata(std::shared_ptr<iceberg::TableMetadataV2> table_metadata) const {
  Ensure(IsRestrictionsTableMetadataAssigned(), "Restrictions Table metadata are not assigned yet");
  auto restrictions = restrictions_table_metadata_.value();
  Ensure(!table_metadata->partition_specs.empty() || !restrictions.partitioned.contains(table_metadata->table_uuid),
         "No partition spec");
  for (auto& i : restrictions.properties) {
    auto iter = table_metadata->properties.find(i.first);
    Ensure(iter != table_metadata->properties.end(), "Missing required properties");
    if (i.second.has_value()) {
      Ensure(iter->second == i.second.value(), "Wrong required properties");
    }
  }
  Ensure(!!table_metadata->GetSortOrder() || !restrictions.sorted.contains(table_metadata->table_uuid),
         "No sort order");
  for (const auto schema : table_metadata->schemas) {
    for (const auto& column : schema->Columns()) {
      std::shared_ptr<const iceberg::types::Type> cur_type = column.type;
      Ensure(!restrictions.forbidden_types.contains(cur_type->TypeId()), "Forbidden type");
      for (int32_t dim = 0; dim < restrictions.max_array_dimensionality && cur_type->IsListType(); ++dim) {
        cur_type = std::dynamic_pointer_cast<const iceberg::types::ListType>(cur_type)->ElementType();
      }
      Ensure(!cur_type->IsListType(), "Array has too many dimensions");
    }
  }
  for (const auto& name : restrictions.required_tags) {
    Ensure(table_metadata->refs.contains(name), "Required tag/branch is missed");
  }
}

void Validator::ValidateManifests(const std::vector<Manifest>& manifests) const {
  Ensure(IsRestrictionsManifestsAssigned(), "Restrictions manifests are not assigned yet");
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
        Ensure(equality_deletes_count <= restrictions.max_equality_deletes_count, "Too many equality deletes");
        ++delete_files_count;
        Ensure(delete_files_count <= restrictions.max_delete_files_count, "Too many delete files");
      }
      if (helper.IsPositionDeletes()) {
        ++position_deletes_count;
        Ensure(position_deletes_count <= restrictions.max_position_deletes_count, "Too many position deletes");
        ++delete_files_count;
        Ensure(delete_files_count <= restrictions.max_delete_files_count, "Too many delete files");
      }
      if (helper.IsData()) {
        ++data_files_count;
        Ensure(data_files_count <= restrictions.max_data_files_count, "Too many data files");
      }
      Ensure(manifest_entry.data_file.file_format == "PARQUET", "Unsupported file format");
      size_t num_row_groups = manifest_entry.data_file.split_offsets.size();
      Ensure(num_row_groups <= restrictions.max_row_groups_per_file, "Too many row groups");
      std::vector<int64_t> row_group_sizes(num_row_groups);
      int64_t sum = 0;
      for (size_t i = 0; i + 1 < num_row_groups; i++) {
        row_group_sizes[i] =
            (manifest_entry.data_file.split_offsets[i + 1] - manifest_entry.data_file.split_offsets[i]) / CHAR_BIT;
        sum += row_group_sizes[i];
      }
      row_group_sizes.back() = manifest_entry.data_file.file_size_in_bytes - sum;
      std::sort(row_group_sizes.begin(), row_group_sizes.end());
      Ensure(row_group_sizes[restrictions.min_row_group_size_percentile * (num_row_groups - 1) / 100] >=
                 restrictions.min_row_group_size_limitation,
             "row group size is too small");
      Ensure(row_group_sizes[restrictions.max_row_group_size_percentile * (num_row_groups - 1) / 100] <=
                 restrictions.max_row_group_size_limitation,
             "row group size is too big");
    }
  }
}

void Validator::AssignRestrictionsTableMetadata(const RestrictionsTableMetadata& restrictions) {
  restrictions_table_metadata_ = restrictions;
}

void Validator::AssignRestrictionsTableMetadata(RestrictionsTableMetadata&& restrictions) {
  restrictions_table_metadata_ = std::move(restrictions);
}

void Validator::AssignRestrictionsManifests(const RestrictionsManifests& restrictions) {
  restrictions_manifests_ = restrictions;
}

void Validator::AssignRestrictionsManifests(RestrictionsManifests&& restrictions) {
  restrictions_manifests_ = std::move(restrictions);
}

bool Validator::IsRestrictionsManifestsAssigned() const { return restrictions_manifests_.has_value(); }

bool Validator::IsRestrictionsTableMetadataAssigned() const { return restrictions_table_metadata_.has_value(); }

RestrictionsTableMetadata& Validator::GetRestrictionsTableMetadata() {
  Ensure(IsRestrictionsTableMetadataAssigned(), "Restrictions Table metadata are not assigned yet");
  return restrictions_table_metadata_.value();
}

const RestrictionsTableMetadata& Validator::GetRestrictionsTableMetadata() const {
  Ensure(IsRestrictionsTableMetadataAssigned(), "Restrictions Table metadata are not assigned yet");
  return restrictions_table_metadata_.value();
}

RestrictionsManifests& Validator::GetRestrictionsManifests() {
  Ensure(IsRestrictionsManifestsAssigned(), "Restrictions Manifests are not assigned yet");
  return restrictions_manifests_.value();
}

const RestrictionsManifests& Validator::GetRestrictionsManifests() const {
  Ensure(IsRestrictionsManifestsAssigned(), "Restrictions Manifests are not assigned yet");
  return restrictions_manifests_.value();
}

static int32_t GetIntArg(const rapidjson::Document& doc, const std::string& name) {
  auto iter = doc.FindMember(name.c_str());
  Ensure(iter != doc.MemberEnd(), "Missing argument: " + name);
  Ensure(iter->value.IsInt(), "Wrong argument type: " + name);
  return iter->value.GetInt();
}

static int64_t GetInt64Arg(const rapidjson::Document& doc, const std::string& name) {
  auto iter = doc.FindMember(name.c_str());
  Ensure(iter != doc.MemberEnd(), "Missing argument: " + name);
  Ensure(iter->value.IsInt64(), "Wrong argument type: " + name);
  return iter->value.GetInt64();
}

static auto GetArray(const rapidjson::Document& doc, const std::string& name) {
  auto iter = doc.FindMember(name.c_str());
  Ensure(iter != doc.MemberEnd(), "Missing argument: " + name);
  Ensure(iter->value.IsArray(), "Wrong argument type: " + name);
  return iter->value.GetArray();
}

RestrictionsTableMetadata ReadRestrictionsTableMetadata(std::istream& istream) {
  rapidjson::Document document;
  rapidjson::IStreamWrapper isw(istream);
  document.ParseStream(isw);
  Ensure(document.IsObject(), "Invalid JSON");
  RestrictionsTableMetadata result{};
  result.max_array_dimensionality = GetIntArg(document, "max_array_dimensionality");
  for (const auto& elem : GetArray(document, "partitioned")) {
    Ensure(elem.IsString(), "Wrong argument type: partitioned");
    result.partitioned.insert(elem.GetString());
  }
  for (const auto& elem : GetArray(document, "sorted")) {
    Ensure(elem.IsString(), "Wrong argument type: sorted");
    result.sorted.insert(elem.GetString());
  }
  for (const auto& elem : GetArray(document, "required_tags")) {
    Ensure(elem.IsString(), "Wrong argument type: required_tags");
    result.required_tags.push_back(elem.GetString());
  }
  auto iter = document.FindMember("properties");
  Ensure(iter != document.MemberEnd(), "Missing argument: properties");
  Ensure(iter->value.IsObject(), "Wrong argument type: properties");
  for (auto& elem : iter->value.GetObject()) {
    Ensure(elem.value.IsString(), "Wrong argument type: properties");
    if (elem.value.GetStringLength() == 0) {
      result.properties.emplace_back(elem.name.GetString(), std::nullopt);
    } else {
      result.properties.emplace_back(elem.name.GetString(), elem.value.GetString());
    }
  }
  for (const auto& elem : GetArray(document, "forbidden_types")) {
    Ensure(elem.IsString(), "Wrong argument type: forbidden_types");
    auto type = types::NameToPrimitiveType(elem.GetString());
    Ensure(type.has_value(), "In forbidden_types: Unknown type: " + static_cast<std::string>(elem.GetString()));
    result.forbidden_types.insert(type.value());
  }
  return result;
}

RestrictionsTableMetadata ReadRestrictionsTableMetadata(const std::string& config_path) {
  std::ifstream ifstream(config_path);
  Ensure(ifstream.is_open(), "Can't open config file");
  return ReadRestrictionsTableMetadata(ifstream);
}

RestrictionsManifests ReadRestrictionsManifests(std::istream& istream) {
  rapidjson::Document document;
  rapidjson::IStreamWrapper isw(istream);
  document.ParseStream(isw);
  Ensure(document.IsObject(), "Invalid JSON");
  RestrictionsManifests result{};
  result.max_equality_deletes_count = GetIntArg(document, "max_equality_deletes_count");
  result.max_position_deletes_count = GetIntArg(document, "max_position_deletes_count");
  result.max_data_files_count = GetIntArg(document, "max_data_files_count");
  result.max_delete_files_count = GetIntArg(document, "max_delete_files_count");
  result.max_row_groups_per_file = GetIntArg(document, "max_row_groups_per_file");
  result.max_row_group_size_percentile = GetIntArg(document, "max_row_group_size_percentile");
  result.min_row_group_size_percentile = GetIntArg(document, "min_row_group_size_percentile");
  result.max_row_group_size_limitation = GetInt64Arg(document, "max_row_group_size_limitation");
  result.min_row_group_size_limitation = GetInt64Arg(document, "min_row_group_size_limitation");
  result.max_metadata_size_per_table = GetInt64Arg(document, "max_metadata_size_per_table");
  return result;
}

RestrictionsManifests ReadRestrictionsManifests(const std::string& config_path) {
  std::ifstream ifstream(config_path);
  Ensure(ifstream.is_open(), "Can't open config file");
  return ReadRestrictionsManifests(ifstream);
}
}  // namespace iceberg::tools