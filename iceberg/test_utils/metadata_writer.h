#pragma once

#include <optional>

#include "arrow/filesystem/localfs.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test_utils/common.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "iceberg/uuid.h"
#include "parquet/arrow/reader.h"

namespace iceberg {

class IcebergMetadataWriter {
 public:
  explicit IcebergMetadataWriter(TableName table_name,
                                 iceberg::PartitionSpec partition_spec = iceberg::PartitionSpec{.spec_id = 0,
                                                                                                .fields = {}})
      : table_name_(std::move(table_name)), partition_spec_(partition_spec) {
    fs_ = std::make_shared<arrow::fs::LocalFileSystem>();  // TODO(gmusya): get in constructor
  }

  // copypasted from other file
  // TODO(gmusya): get rid of it.
  // https://iceberg.apache.org/spec/#partition-transforms
  struct Transforms {
    static constexpr std::string_view kBucket = "bucket";  // actual name is bucket[X]
    static constexpr std::string_view kYear = "year";
    static constexpr std::string_view kMonth = "month";
    static constexpr std::string_view kDay = "day";
    static constexpr std::string_view kHour = "hour";

    static constexpr std::string_view kIdentity = "identity";
    static constexpr std::string_view kTruncate = "truncate";  // actual name is truncate[X]

    static constexpr std::string_view kVoid = "void";

    static arrow::Result<std::shared_ptr<const iceberg::types::Type>> GetTypeFromSourceType(
        int source_id, std::shared_ptr<const iceberg::Schema> schema) {
      for (const auto& column : schema->Columns()) {
        if (column.field_id == source_id) {
          return [&]() -> std::shared_ptr<const iceberg::types::Type> {
            if (column.type->TypeId() == iceberg::TypeID::kTimestamptz) {
              return std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kTimestamp);
            }
            return column.type;
          }();
        }
      }
      return arrow::Status::ExecutionError("Partition spec expects column with field id ", source_id,
                                           " for transform, but this column is missing");
    }
  };

  static std::optional<std::vector<iceberg::ice_tea::PartitionKeyField>> GetFieldsFromPartitionSpec(
      const iceberg::PartitionSpec& partition_spec, std::shared_ptr<const iceberg::Schema> schema) {
    std::vector<iceberg::ice_tea::PartitionKeyField> partition_fields;
    for (const auto& value : partition_spec.fields) {
      const auto& transform = value.transform;
      if (transform.starts_with(Transforms::kBucket) || transform == Transforms::kYear ||
          transform == Transforms::kMonth || transform == Transforms::kHour) {
        partition_fields.emplace_back(value.name,
                                      std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kInt));
      } else if (transform == Transforms::kDay) {
        // https://iceberg.apache.org/spec/#partition-transforms
        // see https://github.com/apache/iceberg/pull/11749
        partition_fields.emplace_back(value.name,
                                      std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kDate));
      } else if (transform == Transforms::kIdentity || transform.starts_with(Transforms::kTruncate)) {
        auto maybe_partition_field_type = Transforms::GetTypeFromSourceType(value.source_id, schema);
        if (!maybe_partition_field_type.ok()) {
          return std::nullopt;
        }
        partition_fields.emplace_back(value.name, maybe_partition_field_type.MoveValueUnsafe());
      } else if (transform == Transforms::kVoid) {
        partition_fields.emplace_back(value.name, nullptr);
      } else {
        return std::nullopt;
      }
    }

    return partition_fields;
  }

  arrow::Status AddFiles(const std::vector<iceberg::FilePath>& paths, iceberg::ContentFile::FileContent file_content,
                         const std::vector<int32_t>& field_ids = {},
                         iceberg::ContentFile::PartitionTuple partition_tuple = {}) {
    iceberg::Manifest manifest;
    for (const auto& path : paths) {
      if (!some_data_path_.has_value()) {
        some_data_path_.emplace(path);
      }
      iceberg::ManifestEntry entry;
      entry.data_file.file_path = path;
      entry.data_file.content = file_content;
      entry.sequence_number = current_sequence_number_;
      entry.file_sequence_number = current_sequence_number_;
      entry.status = iceberg::ManifestEntry::Status::kAdded;
      entry.data_file.partition_tuple = std::move(partition_tuple);

      {
        if (!path.starts_with("file://")) {
          return arrow::Status::ExecutionError("Incorrect path: ", path);
        }
        // tests with missing files are ok
        constexpr uint32_t kSchemaSize = 7;
        auto maybe_input_file = fs_->OpenInputFile(path.substr(kSchemaSize));
        if (maybe_input_file.ok()) {
          auto input_file = maybe_input_file.ValueUnsafe();
          parquet::arrow::FileReaderBuilder reader_builder;
          ARROW_RETURN_NOT_OK(reader_builder.Open(input_file));

          std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
          ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());
          auto meta = arrow_reader->parquet_reader()->metadata();
          const auto row_groups = meta->num_row_groups();
          for (int rg = 0; rg < row_groups; ++rg) {
            auto rg_meta = meta->RowGroup(rg);
            entry.data_file.split_offsets.emplace_back(rg_meta->file_offset());
          }

          entry.data_file.record_count = meta->num_rows();
        }
      }

      if (file_content == iceberg::ContentFile::FileContent::kEqualityDeletes) {
        entry.data_file.equality_ids = field_ids;
      }
      manifest.entries.emplace_back(std::move(entry));
    }
    ++current_sequence_number_;
    iceberg::ManifestFile file;
    ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema());
    auto partition_keys = GetFieldsFromPartitionSpec(partition_spec_, schema);
    if (!partition_keys) {
      return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": internal error in tests");
    }
    std::string data = iceberg::ice_tea::WriteManifestEntries(manifest, *partition_keys);

    std::string local_fs_path = metadata_directory_.path().string() + "/manifest" + std::to_string(file_number_++);
    file.path = "file://" + local_fs_path;
    ARROW_ASSIGN_OR_RAISE(auto os, fs_->OpenOutputStream(local_fs_path));
    ARROW_RETURN_NOT_OK(os->Write(data));
    manifest_files_.emplace_back(std::move(file));
    return arrow::Status::OK();
  }

  arrow::Status AddDataFiles(const std::vector<iceberg::FilePath>& paths) {
    return AddFiles(paths, iceberg::ContentFile::FileContent::kData);
  }

  arrow::Status AddPositionalDeleteFiles(const std::vector<iceberg::FilePath>& paths) {
    return AddFiles(paths, iceberg::ContentFile::FileContent::kPositionDeletes);
  }

  arrow::Status AddEqualityDeleteFiles(const std::vector<iceberg::FilePath>& paths,
                                       const std::vector<int32_t>& field_ids) {
    return AddFiles(paths, iceberg::ContentFile::FileContent::kEqualityDeletes, field_ids);
  }

  static arrow::Result<std::shared_ptr<const iceberg::types::Type>> ConvertPhysicalType(
      const parquet::ColumnDescriptor* column) {
    using PrimitiveType = iceberg::types::PrimitiveType;

    auto physical_type = column->physical_type();

    switch (physical_type) {
      case parquet::Type::BOOLEAN:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kBoolean);
      case parquet::Type::INT32:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kInt);
      case parquet::Type::INT64:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kLong);
      case parquet::Type::FLOAT:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kFloat);
      case parquet::Type::DOUBLE:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kDouble);
      case parquet::Type::BYTE_ARRAY:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kBinary);
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kFixed);
      default:
        break;
    }
    return arrow::Status::ExecutionError("Not supported physical type " + std::to_string(physical_type) +
                                         " for column " + column->name());
  }

  static arrow::Result<std::shared_ptr<const iceberg::types::Type>> IceTypeFromColumn(
      const parquet::ColumnDescriptor* column) {
    using ParquetLogicalType = parquet::LogicalType::Type;
    using PrimitiveType = iceberg::types::PrimitiveType;

    auto& parquet_logical_type = column->logical_type();

    switch (parquet_logical_type->type()) {
      case ParquetLogicalType::STRING:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kString);
      case ParquetLogicalType::DATE:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kDate);
      case ParquetLogicalType::TIME:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTime);
      case ParquetLogicalType::TIMESTAMP: {
        auto timestamp_type = std::static_pointer_cast<const parquet::TimestampLogicalType>(parquet_logical_type);
        if (timestamp_type->is_adjusted_to_utc()) {
          return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTimestamptz);
        } else {
          return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTimestamp);
        }
      }
      case ParquetLogicalType::INT: {
        auto int_logical_type = std::static_pointer_cast<const parquet::IntLogicalType>(parquet_logical_type);
        if (!int_logical_type->is_signed()) {
          return arrow::Status::ExecutionError("Only signed integers are supported");
        }
        const auto bit_width = int_logical_type->bit_width();
        if (bit_width == 16 || bit_width == 32) {
          return std::make_shared<const PrimitiveType>(iceberg::TypeID::kInt);
        }
        if (bit_width == 64) {
          return std::make_shared<const PrimitiveType>(iceberg::TypeID::kLong);
        }
        return arrow::Status::ExecutionError("Integer with bit width ", bit_width, " is not supported");
      }
      case ParquetLogicalType::JSON:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kString);
      case ParquetLogicalType::UUID:
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kUuid);
      case ParquetLogicalType::DECIMAL: {
        auto decimal_logical_type = std::static_pointer_cast<const parquet::DecimalLogicalType>(parquet_logical_type);
        const auto scale = decimal_logical_type->scale();
        const auto precision = decimal_logical_type->precision();
        return std::make_shared<iceberg::types::DecimalType>(precision, scale);
      }
      case ParquetLogicalType::NONE:
        return ConvertPhysicalType(column);
      default:
        break;
    }

    return arrow::Status::ExecutionError("Not supported logical type " + parquet_logical_type->ToString() +
                                         " for column " + column->name());
  }

  arrow::Result<std::shared_ptr<iceberg::Schema>> GetSchema() const {
    if (!some_data_path_.has_value()) {
      return arrow::Status::ExecutionError("No data file to extract schema");
    }

    auto local_fs_path = *some_data_path_;
    if (local_fs_path.starts_with("file://")) {
      local_fs_path = local_fs_path.substr(std::string("file://").size());
    }

    auto local_fs = std::make_shared<arrow::fs::LocalFileSystem>();
    uint64_t file_size = 0;
    auto parquet_meta = iceberg::ParquetMetadata(local_fs, local_fs_path, file_size);

    auto parquet_schema = parquet_meta->schema();
    int num_columns = parquet_schema->num_columns();

    std::vector<iceberg::types::NestedField> fields;
    fields.reserve(num_columns);

    for (int i = 0; i < num_columns; ++i) {
      const parquet::ColumnDescriptor* column = parquet_schema->Column(i);
      if (!column) {
        return arrow::Status::ExecutionError("No column with number " + std::to_string(i));
      }

      ARROW_ASSIGN_OR_RAISE(auto type, IceTypeFromColumn(column));

      const parquet::schema::Node* node = parquet_schema->GetColumnRoot(i);
      bool is_repeated = node->logical_type() && node->logical_type()->is_list();

      if (is_repeated) {
        type = std::make_shared<iceberg::types::ListType>(node->field_id(), false, type);
      }
      iceberg::types::NestedField ice_field{
          .name = node->name(), .field_id = node->field_id(), .is_required = node->is_required(), .type = type};
      fields.emplace_back(std::move(ice_field));
    }

    return std::make_shared<iceberg::Schema>(0, std::move(fields));
  }

  arrow::Result<FilePath> Finalize() {
    iceberg::Snapshot snapshot;
    snapshot.snapshot_id = 1;
    snapshot.summary["operation"] = "append";
    snapshot.schema_id = 0;
    snapshot.sequence_number = current_sequence_number_;

    {
      std::string data = iceberg::ice_tea::WriteManifestList(manifest_files_);
      std::string local_fs_path =
          metadata_directory_.path().string() + "/manifest_list" + std::to_string(file_number_++);
      ARROW_ASSIGN_OR_RAISE(auto os, fs_->OpenOutputStream(local_fs_path));
      ARROW_RETURN_NOT_OK(os->Write(data));

      snapshot.manifest_list_location = "file://" + local_fs_path;
    }

    std::string local_fs_path = metadata_directory_.path().string() + "/snap" + std::to_string(file_number_++);
    std::string location = "file://" + local_fs_path;

    iceberg::TableMetadataV2Builder builder;
    builder.table_uuid = iceberg::UuidGenerator().CreateRandom().ToString();
    builder.snapshots = {std::make_shared<iceberg::Snapshot>(snapshot)};
    builder.current_snapshot_id = 1;
    builder.location = location;
    builder.last_sequence_number = current_sequence_number_;
    builder.last_updated_ms = 0;
    builder.last_column_id = 1;
    ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema());
    builder.schemas = std::vector<std::shared_ptr<iceberg::Schema>>{schema};
    builder.current_schema_id = 0;
    builder.default_spec_id = 0;
    builder.last_partition_id = 0;
    builder.sort_orders = std::vector<std::shared_ptr<iceberg::SortOrder>>{};
    builder.sort_orders->emplace_back(
        std::make_shared<iceberg::SortOrder>(iceberg::SortOrder{.order_id = 0, .fields = {}}));
    builder.default_sort_order_id = 0;
    builder.partition_specs = {std::make_shared<iceberg::PartitionSpec>(partition_spec_)};

    std::shared_ptr<iceberg::TableMetadataV2> meta = builder.Build();
    {
      std::string data = iceberg::ice_tea::WriteTableMetadataV2(*meta);
      ARROW_ASSIGN_OR_RAISE(auto os, fs_->OpenOutputStream(local_fs_path));
      ARROW_RETURN_NOT_OK(os->Write(data));
    }

    return location;
  }

 private:
  iceberg::ScopedTempDir metadata_directory_;
  std::shared_ptr<arrow::fs::LocalFileSystem> fs_;  // TODO(gmusya): custom filesystem
  int32_t file_number_ = 0;
  int32_t current_sequence_number_ = 0;
  const TableName table_name_;
  std::vector<iceberg::ManifestFile> manifest_files_;

  std::optional<iceberg::FilePath> some_data_path_;
  const iceberg::PartitionSpec partition_spec_;
};

}  // namespace iceberg
