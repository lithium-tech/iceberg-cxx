#pragma once

#include <arrow/filesystem/localfs.h>
#include <parquet/type_fwd.h>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace iceberg {

struct ContentFile {
  enum class FileContent {
    kData = 0,
    kPositionDeletes = 1,
    kEqualityDeletes = 2,
  };

  FileContent content;
  std::string file_path;
  std::string file_format;

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

Manifest ReadManifestEntries(std::istream& istream);
Manifest ReadManifestEntries(const std::string& data);
std::string WriteManifestEntries(const Manifest& manifest_entries);

void FillManifestSplitOffsets(std::vector<ManifestEntry>& data, std::shared_ptr<arrow::fs::FileSystem> fs);
void FillManifestSplitOffsets(std::vector<ManifestEntry>& data,
                              const std::vector<std::shared_ptr<parquet::FileMetaData>>& metadata);

}  // namespace ice_tea
}  // namespace iceberg
