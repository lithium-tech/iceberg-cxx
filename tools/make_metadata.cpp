#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>

#include "absl/flags/flag.h"
#include "absl/flags/internal/flag.h"
#include "absl/flags/parse.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/status.h"
#include "iceberg/common/fs/url.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/nested_field.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/streams/arrow/error.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test_utils/write.h"
#include "iceberg/type.h"
#include "iceberg/uuid.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace {

std::shared_ptr<const iceberg::types::Type> ConvertPhysicalType(const parquet::ColumnDescriptor& column) {
  using PrimitiveType = iceberg::types::PrimitiveType;

  auto physical_type = column.physical_type();

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
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kBinary);
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kFixed);
    case parquet::Type::UNDEFINED:
      break;
  }
  throw std::runtime_error("Not supported physical type " + std::to_string(physical_type) + " for column " +
                           column.name());
}

std::shared_ptr<const iceberg::types::Type> ConvertType(const parquet::ColumnDescriptor& column) {
  using ParquetLogicalType = parquet::LogicalType::Type;
  using PrimitiveType = iceberg::types::PrimitiveType;

  auto parquet_logical_type = column.logical_type();
  // TODO(gmusya): check that logical_type() is always non-nullptr
  iceberg::Ensure(parquet_logical_type != nullptr,
                  std::string(__PRETTY_FUNCTION__) + ": internal error. Parquet_logical_type is not set");

  switch (parquet_logical_type->type()) {
    case ParquetLogicalType::MAP:
    case ParquetLogicalType::LIST:
    case ParquetLogicalType::ENUM:
    case ParquetLogicalType::INTERVAL:
    case ParquetLogicalType::NIL:
    case ParquetLogicalType::JSON:
    case ParquetLogicalType::BSON:
    case ParquetLogicalType::UUID:
    case ParquetLogicalType::FLOAT16:
      break;
    case ParquetLogicalType::DECIMAL: {
      auto decimal_type = dynamic_pointer_cast<const parquet::DecimalLogicalType>(parquet_logical_type);
      iceberg::Ensure(decimal_type != nullptr,
                      std::string(__PRETTY_FUNCTION__) + ": internal error. Failed to cast to DecimalLogicalType");

      return std::make_shared<const iceberg::types::DecimalType>(decimal_type->precision(), decimal_type->scale());
    }

    case ParquetLogicalType::STRING:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kString);
    case ParquetLogicalType::DATE:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kDate);
    case ParquetLogicalType::TIME:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTime);
    case ParquetLogicalType::TIMESTAMP: {
      auto timestamp_type = dynamic_pointer_cast<const parquet::TimestampLogicalType>(parquet_logical_type);
      iceberg::Ensure(timestamp_type != nullptr,
                      std::string(__PRETTY_FUNCTION__) + ": internal error. Failed to cast to TimestampLogicalType");
      if (timestamp_type->is_adjusted_to_utc())
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTimestamptz);
      else
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTimestamp);
    }
    case ParquetLogicalType::INT:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kInt);
    case ParquetLogicalType::UNDEFINED:
    case ParquetLogicalType::NONE:
      return ConvertPhysicalType(column);
  }

  throw std::runtime_error("Not supported logical type " + parquet_logical_type->ToString() + " for column " +
                           column.name());
}

std::vector<iceberg::types::NestedField> GetFieldsFromParquet(
    const std::shared_ptr<parquet::FileMetaData>& parquet_meta) {
  using NestedField = iceberg::types::NestedField;
  using PrimitiveType = iceberg::types::PrimitiveType;
  using TypeID = iceberg::TypeID;

  auto parquet_schema = parquet_meta->schema();
  int num_columns = parquet_schema->num_columns();

  std::vector<NestedField> fields;
  fields.reserve(num_columns);

  for (int i = 0; i < num_columns; ++i) {
    const parquet::ColumnDescriptor* column = parquet_schema->Column(i);
    iceberg::Ensure(column != nullptr,
                    std::string(__PRETTY_FUNCTION__) + ": descriptor for column(" + std::to_string(i) + ") is not set");

    const parquet::schema::NodePtr column_schema = column->schema_node();
    iceberg::Ensure(column != nullptr,
                    std::string(__PRETTY_FUNCTION__) + ": column_schema(" + std::to_string(i) + ") is not set");

    NestedField ice_field{.name = column->name(),
                          .field_id = i,
                          .is_required = column_schema->is_required(),
                          .type = ConvertType(*column)};
    fields.emplace_back(std::move(ice_field));
  }

  return fields;
}

struct S3FinalizerGuard {
  S3FinalizerGuard() {
    if (auto status = arrow::fs::InitializeS3(arrow::fs::S3GlobalOptions{}); !status.ok()) {
      throw status;
    }
  }

  ~S3FinalizerGuard() {
    try {
      if (arrow::fs::IsS3Initialized() && !arrow::fs::IsS3Finalized()) {
        arrow::fs::EnsureS3Finalized().ok();
      }
    } catch (...) {
    }
  }
};

}  // namespace

ABSL_FLAG(std::string, parquet_path, "", "path to parquet file (URI)");

ABSL_FLAG(std::string, table_base_location, "", "location where data and metadata files are stored (URI)");
ABSL_FLAG(std::string, root_snapshot_path, "", "path for .metadata.json file (URI)");
ABSL_FLAG(std::string, table_uuid, "", "");

ABSL_FLAG(std::string, filesystem, "file", "filesystem to use (file or s3)");
ABSL_FLAG(std::string, s3_access_key_id, "", "s3_access_key_id");
ABSL_FLAG(std::string, s3_secret_access_key, "", "s3_secret_access_key");
ABSL_FLAG(std::string, s3_endpoint, "", "s3_endpoint");
ABSL_FLAG(std::string, s3_scheme, "", "s3_scheme");
ABSL_FLAG(std::string, s3_region, "", "s3_region");

int main(int argc, char** argv) {
  std::optional<S3FinalizerGuard> s3_guard;

  try {
    absl::ParseCommandLine(argc, argv);

    const std::string filesystem_type = absl::GetFlag(FLAGS_filesystem);

    std::shared_ptr<arrow::fs::FileSystem> fs;
    if (filesystem_type == "file") {
      fs = std::make_shared<arrow::fs::LocalFileSystem>();
    } else if (filesystem_type == "s3") {
      s3_guard.emplace();
      const std::string access_key = absl::GetFlag(FLAGS_s3_access_key_id);
      const std::string secret_key = absl::GetFlag(FLAGS_s3_secret_access_key);
      auto s3options = arrow::fs::S3Options::FromAccessKey(access_key, secret_key);
      s3options.endpoint_override = absl::GetFlag(FLAGS_s3_endpoint);
      s3options.scheme = absl::GetFlag(FLAGS_s3_scheme);
      s3options.region = absl::GetFlag(FLAGS_s3_region);
      auto maybe_fs = arrow::fs::S3FileSystem::Make(s3options);
      if (!maybe_fs.ok()) {
        std::cerr << maybe_fs.status() << std::endl;
        return 1;
      }
      fs = maybe_fs.MoveValueUnsafe();
    } else {
      throw std::runtime_error("Unexpected filesystem type");
    }

    const std::string fs_schema = filesystem_type + "://";

    const std::string table_base_location = absl::GetFlag(FLAGS_table_base_location);
    iceberg::Ensure(!table_base_location.empty(), "table_base_location is not set");

    iceberg::Ensure(table_base_location.starts_with(fs_schema), "table_base_location must start with fs_schema");

    const std::string parquet_path = absl::GetFlag(FLAGS_parquet_path);
    iceberg::Ensure(!parquet_path.empty(), "parquet_path is not set");

    iceberg::Ensure(
        parquet_path.starts_with(table_base_location),
        "parquet_path (" + parquet_path + ") does not start from table_base_location (" + table_base_location + ")");

    std::vector<iceberg::types::NestedField> fields = [&]() {
      iceberg::UrlComponents components = iceberg::SplitUrl(parquet_path);
      std::shared_ptr<arrow::io::RandomAccessFile> file =
          iceberg::ValueSafe(fs->OpenInputFile(std::string(components.path)));

      std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::Open(file);
      iceberg::Ensure(reader != nullptr, std::string(__PRETTY_FUNCTION__) + ": reader is nullptr");

      std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();
      iceberg::Ensure(metadata != nullptr, std::string(__PRETTY_FUNCTION__) + ": metadata is nullptr");

      return GetFieldsFromParquet(metadata);
    }();

    auto schema = std::make_shared<iceberg::Schema>(0, fields);

    std::string root_snapshot_path = absl::GetFlag(FLAGS_root_snapshot_path);
    if (root_snapshot_path.empty()) {
      std::string uuid = iceberg::UuidGenerator().CreateRandom().ToString();
      root_snapshot_path = table_base_location + "/metadata/00000-" + uuid + ".metadata.json";
    } else {
      iceberg::Ensure(root_snapshot_path.starts_with(table_base_location),
                      "root_snapshot_path (" + root_snapshot_path + ") does not start from table_base_location (" +
                          table_base_location + ")");
    }

    iceberg::TableMetadataV2Builder builder;

    std::string table_uuid = absl::GetFlag(FLAGS_table_uuid);
    if (table_uuid.empty()) {
      builder.table_uuid = iceberg::UuidGenerator().CreateRandom().ToString();
    } else {
      // TODO(gmusya): validate that this is UUID
      builder.table_uuid = table_uuid;
    }

    builder.location = table_base_location;
    builder.last_sequence_number = 0;
    builder.last_updated_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    builder.last_column_id = schema->MaxColumnId();
    builder.schemas = std::vector<std::shared_ptr<iceberg::Schema>>{schema};
    builder.current_schema_id = 0;
    iceberg::PartitionSpec partition_spec{.spec_id = 0, .fields = {}};
    builder.partition_specs =
        std::vector<std::shared_ptr<iceberg::PartitionSpec>>{std::make_shared<iceberg::PartitionSpec>(partition_spec)};
    builder.default_spec_id = 0;
    builder.last_partition_id = 0;
    iceberg::SortOrder order{.order_id = 0, .fields = {}};
    builder.sort_orders = std::vector<std::shared_ptr<iceberg::SortOrder>>{std::make_shared<iceberg::SortOrder>(order)};
    builder.default_sort_order_id = 0;

    std::shared_ptr<iceberg::TableMetadataV2> table_metadata = builder.Build();
    iceberg::Ensure(table_metadata != nullptr, "Failed to build table_metadata");

    std::string bytes = iceberg::ice_tea::WriteTableMetadataV2(*table_metadata, true);
    auto output_stream =
        iceberg::ValueSafe(fs->OpenOutputStream(std::string(iceberg::SplitUrl(root_snapshot_path).path)));

    iceberg::Ensure(output_stream->Write(bytes));
    iceberg::Ensure(output_stream->Close());

    std::cerr << "Result file: " << root_snapshot_path << std::endl;
  } catch (std::exception& ex) {
    std::cerr << ex.what() << std::endl;
    return 1;
  } catch (arrow::Status& ex) {
    std::cerr << ex.ToString() << std::endl;
    return 1;
  }

  return 0;
}