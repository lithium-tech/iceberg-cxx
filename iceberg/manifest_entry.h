#pragma once

#include <arrow/filesystem/localfs.h>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>

#include <parquet/type_fwd.h>

#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "iceberg/type.h"

namespace iceberg {

struct ContentFile {
  enum class FileContent {
    kData = 0,
    kPositionDeletes = 1,
    kEqualityDeletes = 2,
  };

  class PartitionKey {
   public:
    struct Fixed {
      std::vector<uint8_t> bytes;

      auto operator<=>(const Fixed& other) const = default;
    };

    using AvroValueHolder =
        std::variant<std::monostate, bool, int, int64_t, float, double, std::string, std::vector<uint8_t>, Fixed>;

    std::string name;
    AvroValueHolder value;
    std::shared_ptr<iceberg::types::Type> type;

    void Validate() {
      const TypeID type_id = type->TypeId();
      const auto error_message = std::string(__FUNCTION__) + ": internal error. Iceberg type " + type->ToString() +
                                 " is not expected with data type " + std::to_string(value.index());
      if (std::holds_alternative<bool>(value)) {
        Ensure(type_id == TypeID::kBoolean, error_message);
      } else if (std::holds_alternative<int>(value)) {
        Ensure(type_id == TypeID::kInt || type_id == TypeID::kDate, error_message);
      } else if (std::holds_alternative<int64_t>(value)) {
        Ensure(type_id == TypeID::kLong || type_id == TypeID::kTime || type_id == TypeID::kTimestamp ||
                   type_id == TypeID::kTimestamptz || type_id == TypeID::kTimestampNs ||
                   type_id == TypeID::kTimestamptzNs,
               error_message);
      } else if (std::holds_alternative<float>(value)) {
        Ensure(type_id == TypeID::kFloat, error_message);
      } else if (std::holds_alternative<double>(value)) {
        Ensure(type_id == TypeID::kDouble, error_message);
      } else if (std::holds_alternative<std::string>(value)) {
        Ensure(type_id == TypeID::kString, error_message);
      } else if (std::holds_alternative<std::vector<uint8_t>>(value)) {
        Ensure(type_id == TypeID::kBinary, error_message);
      } else if (std::holds_alternative<Fixed>(value)) {
        Ensure(type_id == TypeID::kFixed || type_id == TypeID::kUuid || type_id == TypeID::kDecimal, error_message);
      } else {
        throw std::runtime_error(std::string(__PRETTY_FUNCTION__) +
                                 ": internal error. Unexpected data type with iceberg type " + type->ToString());
      }
    }

    template <typename Type>
    PartitionKey(std::string n, Type v, std::shared_ptr<iceberg::types::Type> t)
        : name(std::move(n)), value(std::move(v)), type(t) {
      if (!type) {
        throw std::runtime_error("PartitionInfoField: type must be set");
      }
      Validate();
    }

    PartitionKey(std::string n) : name(std::move(n)) {}

    bool operator==(const PartitionKey& other) const {
      return name == other.name && value == other.value &&
             (type ? type->ToString() : "null") == (other.type ? other.type->ToString() : "null");
    }

   private:
    static void Ensure(bool condition, const std::string& message) {
      if (!condition) {
        throw std::runtime_error(message);
      }
    }
  };

  struct PartitionTuple {
    // sorted by name
    std::vector<PartitionKey> fields;

    bool operator==(const PartitionTuple& other) const = default;
  };

  FileContent content;
  std::string file_path;
  std::string file_format;

  PartitionTuple partition_tuple;
  // TODO(gmusya): read partition info from file

  int64_t record_count;
  int64_t file_size_in_bytes;
  std::map<int32_t, int64_t> column_sizes;
  std::map<int32_t, int64_t> value_counts;
  std::map<int32_t, int64_t> null_value_counts;
  std::map<int32_t, int64_t> nan_value_counts;
  std::map<int32_t, int64_t> distinct_counts;
  std::map<int32_t, std::vector<uint8_t>> lower_bounds;
  std::map<int32_t, std::vector<uint8_t>> upper_bounds;
  std::vector<uint8_t> key_metadata;
  std::vector<int64_t> split_offsets;
  std::vector<int32_t> equality_ids;
  std::optional<int32_t> sort_order_id;
  std::optional<std::string> referenced_data_file;
};

struct DataFile : public ContentFile {
  DataFile() = default;
};

struct ManifestEntry {
  enum class Status {
    kExisting = 0,
    kAdded = 1,
    kDeleted = 2,
  };

  Status status;

  std::optional<int64_t> snapshot_id;
  std::optional<int64_t> sequence_number;
  std::optional<int64_t> file_sequence_number;

  DataFile data_file;
};

struct Manifest {
  using Metadata = std::map<std::string, std::vector<uint8_t>>;

  Metadata metadata;
  std::vector<iceberg::ManifestEntry> entries;

  void SetMetadata(const std::string& key, const std::string& value) {
    metadata[key] = std::vector<uint8_t>(value.begin(), value.end());
  }

  void UpdateMetadataByContent(const ContentFile::FileContent& content) {
    std::string type = "delete";
    switch (content) {
      case ContentFile::FileContent::kData:
        type = "data";
        break;
      case iceberg::ContentFile::FileContent::kEqualityDeletes:
      case iceberg::ContentFile::FileContent::kPositionDeletes:
        type = "deletes";
    }

    SetMetadata("content", type);
  }
};

std::vector<int64_t> SplitOffsets(std::shared_ptr<parquet::FileMetaData> parquet_meta);

std::shared_ptr<parquet::FileMetaData> ParquetMetadata(std::shared_ptr<arrow::io::RandomAccessFile> input_file);
std::shared_ptr<parquet::FileMetaData> ParquetMetadata(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                       const std::string& file_path, uint64_t& file_size);

namespace ice_tea {

struct PartitionKeyField {
  std::string name;
  std::shared_ptr<const iceberg::types::Type> type;

  PartitionKeyField(std::string n, std::shared_ptr<const iceberg::types::Type> t) : name(std::move(n)), type(t) {}
};

struct DataFileDeserializerConfig {
  bool extract_content = true;
  bool extract_file_path = true;
  bool extract_file_format = true;
  bool extract_partition_tuple = true;
  bool extract_record_count = true;
  bool extract_file_size_in_bytes = true;
  bool extract_column_sizes = true;
  bool extract_value_counts = true;
  bool extract_null_value_counts = true;
  bool extract_nan_value_counts = true;
  bool extract_distinct_counts = true;
  bool extract_lower_bounds = true;
  bool extract_upper_bounds = true;
  // bool extract_key_metadata = true;
  bool extract_split_offsets = true;
  bool extract_equality_ids = true;
  bool extract_sort_order_id = true;
  bool extract_referenced_data_file = true;
};

struct ManifestEntryDeserializerConfig {
  bool extract_status = true;
  bool extract_snapshot_id = true;
  bool extract_sequence_number = true;
  bool extract_file_sequence_number = true;

  DataFileDeserializerConfig datafile_config = {};
};

Manifest ReadManifestEntries(std::istream& istream, const ManifestEntryDeserializerConfig& config = {});
Manifest ReadManifestEntries(const std::string& data, const ManifestEntryDeserializerConfig& config = {});
std::string WriteManifestEntries(const Manifest& manifest_entries,
                                 const std::vector<PartitionKeyField>& partition_spec = {});

void FillManifestSplitOffsets(std::vector<ManifestEntry>& data, std::shared_ptr<arrow::fs::FileSystem> fs);
void FillManifestSplitOffsets(std::vector<ManifestEntry>& data,
                              const std::vector<std::shared_ptr<parquet::FileMetaData>>& metadata);

}  // namespace ice_tea
}  // namespace iceberg
