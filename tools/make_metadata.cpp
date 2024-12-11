#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <parquet/metadata.h>

#include <algorithm>
#include <filesystem>

#include "tools/common.h"
#include "tools/metadata_tree.h"

using iceberg::tools::MetadataTree;
using iceberg::tools::SnapshotMaker;

namespace {

std::vector<std::filesystem::path> ListFiles(const std::filesystem::path& src, const std::string& extension = "") {
  if (!std::filesystem::exists(src)) {
    throw std::runtime_error("Not exists: " + src.string());
  }

  if (std::filesystem::is_regular_file(src)) {
    std::filesystem::path path(src);
    if (path.extension() != extension) {
      throw std::runtime_error("Unexpected file type: " + src.string());
    }
    return {path};
  }

  if (!std::filesystem::is_directory(src)) {
    throw std::runtime_error("Not a directory: " + src.string());
  }

  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(src)) {
    if (entry.is_regular_file() && (extension.empty() || entry.path().extension() == extension)) {
      files.emplace_back(entry.path());
    }
  }
  std::sort(files.begin(), files.end());
  return files;
}

std::vector<std::string> MakeDstPaths(const std::vector<std::filesystem::path>& srcs, const std::string& dst,
                                      const std::string& data_dir) {
  std::vector<std::string> out;
  out.reserve(srcs.size());
  for (auto& path : srcs) {
    if (data_dir.empty()) {
      out.push_back(dst / path.filename());
    } else {
      out.push_back(dst / (data_dir / path.filename()));
    }
  }
  return out;
}

std::shared_ptr<const iceberg::types::Type> ConvertPhysicalType(const parquet::ColumnDescriptor* column,
                                                                bool byte_array_is_string) {
  using PrimitiveType = iceberg::types::PrimitiveType;

  auto physical_type = column->physical_type();

  switch (physical_type) {
    case parquet::Type::BOOLEAN:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kBoolean);
    case parquet::Type::INT32:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kInt);
    case parquet::Type::INT64:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kLong);
    case parquet::Type::INT96:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kBinary);
    case parquet::Type::FLOAT:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kFloat);
    case parquet::Type::DOUBLE:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kDouble);
    case parquet::Type::BYTE_ARRAY:
      if (byte_array_is_string) {
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kString);
      } else {
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kBinary);
      }
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kFixed);
    case parquet::Type::UNDEFINED:
      break;
  }
  throw std::runtime_error("Not supported physical type " + std::to_string(physical_type) + " for column " +
                           column->name());
}

std::shared_ptr<const iceberg::types::Type> ConvertType(const parquet::ColumnDescriptor* column,
                                                        bool byte_array_is_string) {
  using ParquetLogicalType = parquet::LogicalType::Type;
  using PrimitiveType = iceberg::types::PrimitiveType;

  auto& parquet_logical_type = column->logical_type();

  std::string str_type;
  switch (parquet_logical_type->type()) {
    case ParquetLogicalType::MAP:
    case ParquetLogicalType::LIST:
    case ParquetLogicalType::ENUM:
    case ParquetLogicalType::DECIMAL:
    case ParquetLogicalType::INTERVAL:
    case ParquetLogicalType::NIL:
    case ParquetLogicalType::JSON:
    case ParquetLogicalType::BSON:
    case ParquetLogicalType::UUID:
    case ParquetLogicalType::FLOAT16:
      str_type = parquet_logical_type->ToString();
      break;
    case ParquetLogicalType::STRING:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kString);
    case ParquetLogicalType::DATE:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kDate);
    case ParquetLogicalType::TIME:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTime);
    case ParquetLogicalType::TIMESTAMP: {
      auto& timestamp_type = dynamic_cast<const parquet::TimestampLogicalType&>(*parquet_logical_type);
      if (timestamp_type.is_adjusted_to_utc())
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTimestamptz);
      else
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTimestamp);
    }
    case ParquetLogicalType::INT:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kInt);
    case ParquetLogicalType::UNDEFINED:
    case ParquetLogicalType::NONE:
      return ConvertPhysicalType(column, byte_array_is_string);
  }

  throw std::runtime_error("Not supported logical type " + parquet_logical_type->ToString() + " for column " +
                           column->name());
}

std::vector<iceberg::types::NestedField> GetFieldsFromParquet(
    const std::shared_ptr<parquet::FileMetaData>& parquet_meta, bool not_null, bool byte_array_is_string) {
  using NestedField = iceberg::types::NestedField;
  using PrimitiveType = iceberg::types::PrimitiveType;
  using TypeID = iceberg::TypeID;

  auto parquet_schema = parquet_meta->schema();
  int num_columns = parquet_schema->num_columns();

  std::vector<NestedField> fields;
  fields.reserve(num_columns);

  for (int i = 0; i < num_columns; ++i) {
    const parquet::ColumnDescriptor* column = parquet_schema->Column(i);
    if (!column) {
      throw std::runtime_error("No column with number " + std::to_string(i));
    }

    NestedField ice_field{.name = column->name(),
                          .field_id = i,
                          .is_required = not_null,
                          .type = ConvertType(column, byte_array_is_string)};
    fields.emplace_back(std::move(ice_field));
  }

  return fields;
}

}  // namespace

ABSL_FLAG(std::string, src_data, "", "local path to source data parquet file or directory with parquet files");
// TODO: src_deletes
ABSL_FLAG(std::string, dst, "", "logical destination absolute path (i.e. s3://my_bucket/clickbench)");
ABSL_FLAG(std::string, data_dir, "", "path to dst data subdirectory (related to dst)");
ABSL_FLAG(std::string, meta_dir, "metadata", "path to dst metadata subdirectory (related to dst)");
ABSL_FLAG(std::string, meta_uuid, "", "UUID for metadata.json file");
ABSL_FLAG(bool, force_not_null, false, "make columns NOT NULL");
ABSL_FLAG(bool, byte_array_is_string, false, "intarpret BYTE_ARRAY type as String (Binary if not set)");

int main(int argc, char** argv) {
  try {
    static const std::string meta_ext = ".metadata.json";

    absl::ParseCommandLine(argc, argv);

    const std::string src_data = absl::GetFlag(FLAGS_src_data);
    const std::string dst = absl::GetFlag(FLAGS_dst);
    const std::string data_dir = absl::GetFlag(FLAGS_data_dir);
    const std::string meta_dir = absl::GetFlag(FLAGS_meta_dir);
    std::string meta_uuid = absl::GetFlag(FLAGS_meta_uuid);
    const bool force_not_null = absl::GetFlag(FLAGS_force_not_null);
    const bool byte_array_is_string = absl::GetFlag(FLAGS_byte_array_is_string);

    if (src_data.empty()) {
      throw std::runtime_error("No src specified");
    }
    if (dst.empty()) {
      throw std::runtime_error("No dst directory specified");
    }
    if (meta_dir.empty() || std::filesystem::exists(meta_dir)) {
      throw std::runtime_error("Wrong meta_dir (empty or exists): " + meta_dir);
    }
    if (meta_uuid.empty()) {
      meta_uuid = iceberg::UuidGenerator().CreateRandom().ToString();
    }

    auto src_data_files = ListFiles(src_data, ".parquet");
    auto dst_data_files = MakeDstPaths(src_data_files, dst, data_dir);
    if (dst_data_files.empty()) {
      throw std::runtime_error("No src files found");
    }

    std::filesystem::create_directory(meta_dir);
    std::filesystem::path data_location = dst / std::filesystem::path(data_dir);
    std::filesystem::path metadata_location = dst / std::filesystem::path(meta_dir);
    std::filesystem::path metadata_json = metadata_location / (meta_uuid + meta_ext);

    auto local_fs = std::make_shared<arrow::fs::LocalFileSystem>();
    uint64_t file_size = 0;
    auto parquet_meta = iceberg::ParquetMetadata(local_fs, src_data_files[0], file_size);

    std::vector<iceberg::types::NestedField> fields =
        GetFieldsFromParquet(parquet_meta, force_not_null, byte_array_is_string);
    auto schema = std::make_shared<iceberg::Schema>(0, fields);
    auto empty_tree = MetadataTree::MakeEmptyMetadataFile(meta_uuid, metadata_json, schema);

    auto snapshot_maker = SnapshotMaker(local_fs, empty_tree.table_metadata, 0);

    snapshot_maker.MakeMetadataFiles(meta_dir, src_data, metadata_location, data_location, {}, dst_data_files, {}, 0);
  } catch (std::exception& ex) {
    std::cerr << ex.what() << std::endl;
    return 1;
  }

  return 0;
}