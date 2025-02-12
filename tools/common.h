#pragma once

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>
#include <parquet/type_fwd.h>

#include <cinttypes>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/uuid.h"
#include "tools/metadata_tree.h"

namespace iceberg::tools {

void EnsureSameSchema(std::shared_ptr<iceberg::Schema> src_schema, std::shared_ptr<iceberg::Schema> dst_schema,
                      bool require_same_id = false, bool require_same_field_ids = true);
void EnsureSameSortOrder(std::shared_ptr<iceberg::SortOrder> src_order, std::shared_ptr<iceberg::SortOrder> dst_order,
                         bool require_same_id = false, bool require_same_transform = false);

iceberg::ContentFile RowGroupStats(const parquet::RowGroupMetaData& rg_meta);

inline void AppendStats(iceberg::ContentFile& file_stats, const iceberg::ContentFile& rg_stats) {
  for (auto& [column, value] : rg_stats.column_sizes) {
    file_stats.column_sizes[column] += value;
  }
  for (auto& [column, value] : rg_stats.value_counts) {
    file_stats.value_counts[column] += value;
  }
  for (auto& [column, value] : rg_stats.null_value_counts) {
    file_stats.null_value_counts[column] += value;
  }
  for (auto& [column, value] : rg_stats.nan_value_counts) {
    file_stats.nan_value_counts[column] += value;
  }
  // cannot merge distinct_counts
  // TODO(chertus): merge lower_bounds
  // TODO(chertus): merge upper_bounds
}

struct FileCounts {
  int32_t data_files = 0;
  int32_t pos_delete_files = 0;
  int32_t eq_delete_files = 0;
  int64_t data_rows = 0;
  int64_t pos_delete_rows = 0;
  int64_t eq_delete_rows = 0;
  int64_t files_size = 0;

  int32_t Files() const { return data_files + pos_delete_files + eq_delete_files; }
  int32_t DeleteFiles() const { return pos_delete_files + eq_delete_files; }
  int64_t Rows() const { return data_rows + pos_delete_rows + eq_delete_rows; }

  void Append(const iceberg::ManifestEntry& entry) {
    switch (entry.data_file.content) {
      case iceberg::ContentFile::FileContent::kData:
        ++data_files;
        data_rows += entry.data_file.record_count;
        break;
      case iceberg::ContentFile::FileContent::kPositionDeletes:
        ++pos_delete_files;
        pos_delete_rows += entry.data_file.record_count;
        break;
      case iceberg::ContentFile::FileContent::kEqualityDeletes:
        ++eq_delete_files;
        eq_delete_rows += entry.data_file.record_count;
        break;
    }
    files_size += entry.data_file.file_size_in_bytes;
  }

  FileCounts operator+(const FileCounts& other) const {
    return FileCounts{.data_files = data_files + other.data_files,
                      .pos_delete_files = pos_delete_files + other.pos_delete_files,
                      .eq_delete_files = eq_delete_files + other.eq_delete_files,
                      .data_rows = data_rows + other.data_rows,
                      .pos_delete_rows = pos_delete_rows + other.pos_delete_rows,
                      .eq_delete_rows = eq_delete_rows + other.eq_delete_rows};
  }

  static FileCounts FromSummary(std::map<std::string, std::string>& summary) {
    FileCounts out;
    out.files_size = std::atoll(summary["total-files-size"].data());
    out.data_files = std::atoll(summary["total-data-files"].data());
    out.pos_delete_files = 0;
    out.eq_delete_files = std::atoll(summary["total-delete-files"].data());
    out.pos_delete_rows = std::atoll(summary["total-position-deletes"].data());
    out.eq_delete_rows = std::atoll(summary["total-equality-deletes"].data());
    out.data_rows = std::atoll(summary["total-records"].data()) - out.pos_delete_rows - out.eq_delete_rows;
    return out;
  }
};

struct FileStats {
  FileCounts added;
  FileCounts deleted;
  FileCounts existing;

  FileStats operator+(const FileStats& other) const {
    return FileStats{
        .added = added + other.added, .deleted = deleted + other.deleted, .existing = existing + other.existing};
  }

  int64_t TotalFilesSize() const { return added.files_size + existing.files_size - deleted.files_size; }
  int32_t TotalDataFiles() const { return added.data_files + existing.data_files - deleted.data_files; }
  int32_t TotalDeleteFiles() const { return added.DeleteFiles() + existing.DeleteFiles() - deleted.DeleteFiles(); }

  int64_t TotalRecords() const { return added.Rows() + existing.Rows() - deleted.Rows(); }
  int64_t TotalPositionDeletes() const {
    return added.pos_delete_rows + existing.pos_delete_rows - deleted.pos_delete_rows;
  }
  int64_t TotalEqualityDeletes() const {
    return added.eq_delete_rows + existing.eq_delete_rows - deleted.eq_delete_rows;
  }
};

struct MetadataNameMaker {
  Uuid uuid_metadata = Uuid(1);
  Uuid uuid_snapshot = Uuid(2);
  Uuid uuid_added_data_manifest = Uuid(3);
  Uuid uuid_added_delete_manifest = Uuid(4);

  static std::string StrUUID(__int128 x) {
    uint32_t p0 = x >> (12 * 8);
    uint16_t p1 = x >> (10 * 8);  // & 0xffff;
    uint16_t p2 = x >> (8 * 8);   // & 0xffff;
    uint16_t p3 = x >> (6 * 8);   // & 0xffff;
    uint64_t p4 = x & 0xfffffffffffful;
#if 0
    return std::format("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", p0, p1, p2, p3, p4);
#else
    char buf[1024];
    snprintf(buf, sizeof(buf), "%08x-%04x-%04x-%04x-%012" PRIx64, p0, p1, p2, p3, p4);
    return buf;
#endif
  }

  static std::string MetadataName(int counter, Uuid uuid) {
#if 0
    return std::format("{:05}-{}.metadata.json", counter, uuid.ToString());
#else
    char buf[1024];
    snprintf(buf, sizeof(buf), "%05d-%s.metadata.json", counter, uuid.ToString().c_str());
    return buf;
#endif
  }

  static std::string SnapshotName(int64_t snap_id, Uuid uuid) {
#if 0
    return std::format("snap-{}-{}-{}.avro", snap_id, 1, uuid.ToString());
#else
    char buf[1024];
    snprintf(buf, sizeof(buf), "snap-%" PRId64 "-%d-%s.avro", snap_id, 1, uuid.ToString().c_str());
    return buf;
#endif
  }

  static std::string ManifestName(Uuid uuid, int mode) {
#if 0
    return std::format("{}-m{}.avro", StrUUID(uuid), mode);
#else
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s-m%d.avro", uuid.ToString().c_str(), mode);
    return buf;
#endif
  }
};

struct SnapshotMaker {
  std::shared_ptr<iceberg::TableMetadataV2> table_metadata;
  std::shared_ptr<iceberg::Snapshot> parent_snap;
  MetadataNameMaker name_maker;
  mutable UuidGenerator uuid_generator;
  std::shared_ptr<arrow::fs::FileSystem> fs;

  SnapshotMaker(std::shared_ptr<arrow::fs::FileSystem> fs_,
                const std::shared_ptr<iceberg::TableMetadataV2>& prev_table_metadata, int64_t current_time_ms,
                int meta_seqno = 0);

  void MakeMetadataFiles(const std::filesystem::path& out_metadata_location,
                         const std::filesystem::path& local_data_location,
                         const std::filesystem::path& metadata_location, const std::filesystem::path& data_location,
                         const std::unordered_map<std::string, std::shared_ptr<Manifest>>& existing,
                         const std::vector<std::string>& added_data_files,
                         const std::vector<std::string>& added_delete_files, int64_t schema_id,
                         const std::optional<std::string>& result_metadata_path = std::nullopt) {
    Manifest added_data_entries =
        MakeEntries(local_data_location, data_location, added_data_files, iceberg::ContentFile::FileContent::kData);
    Manifest added_delete_entries = MakeEntries(local_data_location, data_location, added_delete_files,
                                                iceberg::ContentFile::FileContent::kEqualityDeletes);

    MakeMetadataFiles(out_metadata_location, metadata_location, existing, added_data_entries, added_delete_entries,
                      schema_id, result_metadata_path);
  }

 private:
  int SnapshotId() const { return *table_metadata->current_snapshot_id; }
  int64_t MetaSeqno() const { return table_metadata->last_sequence_number; }
  int64_t CurrentTimeMs() const { return table_metadata->last_updated_ms; }
  std::shared_ptr<iceberg::SortOrder> SortOrder() const { return table_metadata->GetSortOrder(); }

  void MakeMetadataFiles(const std::filesystem::path& out_metadata_location,
                         const std::filesystem::path& metadata_location,
                         const std::unordered_map<std::string, std::shared_ptr<Manifest>>& existing,
                         const Manifest& added_data_entries, const Manifest& added_delete_entries, int64_t schema_id,
                         const std::optional<std::string>& result_metadata_path);

  Manifest MakeEntries(const std::filesystem::path& local_data_location, const std::filesystem::path& data_location,
                       const std::vector<std::string>& files, iceberg::ContentFile::FileContent content) const;

  static iceberg::ManifestFile MakeManifest(const std::string& path, int64_t snapshot_id, int seqno,
                                            const std::vector<iceberg::ManifestEntry>& added_entries,
                                            const std::vector<iceberg::ManifestEntry>& deleted_entries,
                                            const std::vector<iceberg::ManifestEntry>& existing_entries,
                                            iceberg::ManifestContent content, FileStats& stats);

  iceberg::ManifestFile MakeDataManifest(const std::string& path,
                                         const std::vector<iceberg::ManifestEntry>& added_entries,
                                         const std::vector<iceberg::ManifestEntry>& deleted_entries,
                                         const std::vector<iceberg::ManifestEntry>& existing_entries,
                                         FileStats& stats) const {
    return MakeManifest(path, SnapshotId(), MetaSeqno(), added_entries, deleted_entries, existing_entries,
                        iceberg::ManifestContent::kData, stats);
  }

  iceberg::ManifestFile MakeEqDeleteManifest(const std::string& path,
                                             const std::vector<iceberg::ManifestEntry>& added_entries,
                                             const std::vector<iceberg::ManifestEntry>& deleted_entries,
                                             const std::vector<iceberg::ManifestEntry>& existing_entries,
                                             FileStats& stats) const {
    return MakeManifest(path, SnapshotId(), MetaSeqno(), added_entries, deleted_entries, existing_entries,
                        iceberg::ManifestContent::kDeletes, stats);
  }

  static std::map<std::string, std::string> MakeSummary(const FileStats& stats);
  std::shared_ptr<iceberg::Snapshot> MakeSnapshot(const std::string& path, const FileStats& stats) const;

  std::string MakeMetadataName() const { return name_maker.MetadataName(MetaSeqno(), uuid_generator.CreateRandom()); }
  std::string MakeSnapshotName() const { return name_maker.SnapshotName(SnapshotId(), uuid_generator.CreateRandom()); }
};

}  // namespace iceberg::tools
