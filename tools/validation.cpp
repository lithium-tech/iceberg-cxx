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
bool Restrictions::operator==(const Restrictions& other) const = default;

Restrictions::Restrictions() {}

Restrictions::Restrictions(int32_t max_equality_deletes_count_, int32_t max_position_deletes_count_,
                           int32_t max_data_files_count_, int32_t max_delete_files_count_,
                           int32_t max_row_groups_per_file_, int32_t max_array_dimensionality_,
                           int32_t max_row_group_size_percentile_, int32_t min_row_group_size_percentile_,
                           int64_t max_row_group_size_limitation_, int64_t min_row_group_size_limitation_,
                           int64_t max_metadata_size_per_table_, std::unordered_set<std::string>&& partitioned_,
                           std::unordered_set<std::string>&& sorted, std::vector<std::string>&& required_tags_,
                           std::vector<std::string>&& required_statistics_,
                           std::vector<std::pair<std::string, std::optional<std::string>>>&& properties_,
                           std::unordered_set<iceberg::TypeID>&& forbidden_types_)
    : max_equality_deletes_count(max_equality_deletes_count_),
      max_position_deletes_count(max_position_deletes_count_),
      max_data_files_count(max_data_files_count_),
      max_delete_files_count(max_delete_files_count_),
      max_row_groups_per_file(max_row_groups_per_file_),
      max_array_dimensionality(max_array_dimensionality_),
      max_row_group_size_percentile(max_row_group_size_percentile_),
      min_row_group_size_percentile(min_row_group_size_percentile_),
      max_row_group_size_limitation(max_row_group_size_limitation_),
      min_row_group_size_limitation(min_row_group_size_limitation_),
      max_metadata_size_per_table(max_metadata_size_per_table_),
      partitioned(std::move(partitioned_)),
      sorted(std::move(sorted)),
      required_tags(std::move(required_tags_)),
      required_statistics(std::move(required_statistics_)),
      properties(std::move(properties_)),
      forbidden_types(std::move(forbidden_types_)) {}

Validator::Validator(const Restrictions& restrictions) : restrictions_(restrictions) {}

static void Ensure(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void Validator::ValidateTableMetadata(std::shared_ptr<iceberg::TableMetadataV2> table_metadata) const {
  Ensure(!table_metadata->partition_specs.empty() || !restrictions_.partitioned.contains(table_metadata->table_uuid),
         "No partition spec");
  for (auto& i : restrictions_.properties) {
    auto iter = table_metadata->properties.find(i.first);
    Ensure(iter != table_metadata->properties.end(), "Missing required properties");
    if (i.second.has_value()) {
      Ensure(iter->second == i.second.value(), "Wrong required properties");
    }
  }
  Ensure(!!table_metadata->GetSortOrder() || !restrictions_.sorted.contains(table_metadata->table_uuid),
         "No sort order");
  for (const auto schema : table_metadata->schemas) {
    for (const auto& column : schema->Columns()) {
      std::shared_ptr<const iceberg::types::Type> cur_type = column.type;
      Ensure(!restrictions_.forbidden_types.contains(cur_type->TypeId()), "Forbidden type");
      for (int32_t dim = 0; dim < restrictions_.max_array_dimensionality && cur_type->IsListType(); ++dim) {
        cur_type = std::dynamic_pointer_cast<const iceberg::types::ListType>(cur_type)->ElementType();
      }
      Ensure(!cur_type->IsListType(), "Array has too many dimensions");
    }
  }
  for (const auto& name : restrictions_.required_tags) {
    Ensure(table_metadata->refs.contains(name), "Required tag/branch is missed");
  }
}

void Validator::ValidateManifests(const std::vector<std::shared_ptr<Manifest>>& manifests) const {
  int32_t equality_deletes_count = 0;
  int32_t position_deletes_count = 0;
  int32_t data_files_count = 0;
  int32_t delete_files_count = 0;
  for (const auto& manifest : manifests) {
    for (const auto& manifest_entry : manifest->entries) {
      auto helper = ManifestEntryHelper(manifest_entry);
      if (helper.IsEqualityDeletes()) {
        ++equality_deletes_count;
        Ensure(equality_deletes_count <= restrictions_.max_equality_deletes_count, "Too many equality deletes");
        ++delete_files_count;
        Ensure(delete_files_count <= restrictions_.max_delete_files_count, "Too many delete files");
      }
      if (helper.IsPositionDeletes()) {
        ++position_deletes_count;
        Ensure(position_deletes_count <= restrictions_.max_position_deletes_count, "Too many position deletes");
        ++delete_files_count;
        Ensure(delete_files_count <= restrictions_.max_delete_files_count, "Too many delete files");
      }
      if (helper.IsData()) {
        ++data_files_count;
        Ensure(data_files_count <= restrictions_.max_data_files_count, "Too many data files");
      }
      Ensure(manifest_entry.data_file.file_format == "PARQUET", "Unsupported file format");
      size_t num_row_groups = manifest_entry.data_file.split_offsets.size();
      Ensure(num_row_groups <= restrictions_.max_row_groups_per_file, "Too many row groups");
      std::vector<int64_t> row_group_sizes(num_row_groups);
      int64_t sum = 0;
      for (size_t i = 0; i + 1 < num_row_groups; i++) {
        row_group_sizes[i] =
            (manifest_entry.data_file.split_offsets[i + 1] - manifest_entry.data_file.split_offsets[i]) / CHAR_BIT;
        sum += row_group_sizes[i];
      }
      row_group_sizes.back() = manifest_entry.data_file.file_size_in_bytes - sum;
      std::sort(row_group_sizes.begin(), row_group_sizes.end());
      Ensure(row_group_sizes[restrictions_.min_row_group_size_percentile * (num_row_groups - 1) / 100] >=
                 restrictions_.min_row_group_size_limitation,
             "row group size is too small");
      Ensure(row_group_sizes[restrictions_.max_row_group_size_percentile * (num_row_groups - 1) / 100] <=
                 restrictions_.max_row_group_size_limitation,
             "row group size is too big");
    }
  }
}

/*void Validator::ValidateParquets(const std::vector<std::shared_ptr<Manifest>>& manifests) const {
    int64_t metadata_size = 0;
    for (const auto &manifest : manifests) {
        for (const auto &manifest_entry : manifest->entries) {
            Ensure(manifest_entry.data_file.file_format == "PARQUET", "Unsupported file format");
            auto reader = parquet::ParquetFileReader::OpenFile(manifest_entry.data_file.file_path); // automatically
closes on destruction auto parquet_metadata = reader->metadata(); for (auto& statistics :
restrictions_.required_statistics) { Ensure(parquet_metadata->key_value_metadata()->Contains(statistics), "Missing
required statistics");
            }
            metadata_size += parquet_metadata->size();
            Ensure(metadata_size <= restrictions_.max_metadata_size_per_table, "Metadata size is too big");
            Ensure(manifest_entry.data_file.split_offsets.size() == parquet_metadata->num_row_groups(), "Split offsets
size doesn't match number of row groups");
        }
    }
}*/

Restrictions& Validator::GetRestrictions() { return restrictions_; }

const Restrictions& Validator::GetRestrictions() const { return restrictions_; }

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

Restrictions ReadRestrictions(std::istream& istream) {
  rapidjson::Document document;
  rapidjson::IStreamWrapper isw(istream);
  document.ParseStream(isw);
  Ensure(document.IsObject(), "Invalid JSON");
  Restrictions result{};
  result.max_equality_deletes_count = GetIntArg(document, "max_equality_deletes_count");
  result.max_position_deletes_count = GetIntArg(document, "max_position_deletes_count");
  result.max_data_files_count = GetIntArg(document, "max_data_files_count");
  result.max_delete_files_count = GetIntArg(document, "max_delete_files_count");
  result.max_row_groups_per_file = GetIntArg(document, "max_row_groups_per_file");
  result.max_array_dimensionality = GetIntArg(document, "max_array_dimensionality");
  result.max_row_group_size_percentile = GetIntArg(document, "max_row_group_size_percentile");
  result.min_row_group_size_percentile = GetIntArg(document, "min_row_group_size_percentile");
  result.max_row_group_size_limitation = GetInt64Arg(document, "max_row_group_size_limitation");
  result.min_row_group_size_limitation = GetInt64Arg(document, "min_row_group_size_limitation");
  result.max_metadata_size_per_table = GetInt64Arg(document, "max_metadata_size_per_table");
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
  for (const auto& elem : GetArray(document, "required_statistics")) {
    Ensure(elem.IsString(), "Wrong argument type: required_statistics");
    result.required_statistics.push_back(elem.GetString());
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

Restrictions ReadRestrictions(const std::string& config_path) {
  std::ifstream ifstream(config_path);
  Ensure(ifstream.is_open(), "Can't open config file");
  return ReadRestrictions(ifstream);
}
}  // namespace iceberg::tools