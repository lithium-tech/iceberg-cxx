#pragma once

#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/table_metadata.h"

namespace iceberg::tools {

struct StringFix {
  std::string from;
  std::string to;

  bool NeedFix() const { return from != to; }
};

class ManifestEntryHelper {
 public:
  using EntryStatus = iceberg::ManifestEntry::Status;
  using FileContent = iceberg::ContentFile::FileContent;

  explicit ManifestEntryHelper(const iceberg::ManifestEntry& entry) : entry_(entry) {}

  std::filesystem::path FilePath() const { return entry_.data_file.file_path; }
  bool IsDeleted() const { return entry_.status == EntryStatus::kDeleted; }
  bool IsData() const { return entry_.data_file.content == FileContent::kData; }
  bool IsEqualityDeletes() const { return entry_.data_file.content == FileContent::kEqualityDeletes; }
  bool IsPositionDeletes() const { return entry_.data_file.content == FileContent::kPositionDeletes; }

 private:
  const iceberg::ManifestEntry& entry_;
};

using ManifestList = std::vector<iceberg::ManifestFile>;

class MetadataTree {
 public:
  struct MetadataFile {
    std::shared_ptr<iceberg::TableMetadataV2> table_metadata;

    const std::map<std::string, SnapshotRef>& Refs() const { return table_metadata->refs; }
    const std::vector<std::shared_ptr<Snapshot>>& Snapshots() const { return table_metadata->snapshots; }

    void RemoveOtherSnapshots(int64_t snapshot_id, bool switch_main = true);
  };

  static MetadataFile ReadMetadataFile(const std::filesystem::path& path);

  explicit MetadataTree(const std::filesystem::path& path);
  MetadataTree(const std::filesystem::path& path, int64_t snapshot_id);
  MetadataTree(const std::filesystem::path& path, const std::string& ref);

  std::map<int64_t, std::string> MetadataLog() const;

  void FixLocation(const StringFix& fix_paths, std::unordered_map<std::string, std::string>& renames_data,
                   std::unordered_map<std::string, std::string>& renames_meta,
                   std::unordered_map<std::string, std::string>& rename_locations);

  MetadataFile& GetMetadataFile() { return medatada_file; }
  const std::string& Location() const { return medatada_file.table_metadata->location; }
  const MetadataFile& GetMetadataFile() const { return medatada_file; }
  const auto& GetManifests() const { return manifests; }
  std::string SerializeMetadataFile() const;
  void WriteFiles(const std::filesystem::path& out_dir) const;
  void Print(std::ostream& os, size_t limit_files = 2) const;

 private:
  std::filesystem::path medatada_file_path;
  MetadataFile medatada_file;
  std::unordered_map<std::string, std::shared_ptr<ManifestList>> manifests_lists;
  std::unordered_map<std::string, std::shared_ptr<Manifest>> manifests;

  void AddSnapshot(const std::shared_ptr<Snapshot>& snap, const std::filesystem::path& files_path);

  friend std::ostream& operator<<(std::ostream& os, const MetadataTree& meta_tree) {
    meta_tree.Print(os);
    return os;
  }
};

void LoadTree(MetadataTree& meta_tree, const std::filesystem::path& metadata_path, std::vector<MetadataTree>& prev_meta,
              bool ignore_missing_snapshots);
void LoadTree(MetadataTree& meta_tree, const std::filesystem::path& metadata_path, std::vector<MetadataTree>& prev_meta,
              std::unordered_map<std::string, std::string>& renames_data,
              std::unordered_map<std::string, std::string>& renames_meta,
              std::unordered_map<std::string, std::string>& rename_locations, bool ignore_missing_snapshots);

}  // namespace iceberg::tools
