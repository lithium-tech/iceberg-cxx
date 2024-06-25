#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/src/manifest_entry.h"
#include "iceberg/src/manifest_file.h"
#include "iceberg/src/table_metadata.h"

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

class MetadataTree {
 public:
  struct MetadataFile {
    std::shared_ptr<iceberg::TableMetadataV2> table_metadata;

    size_t ListsCount() const { return table_metadata->snapshots.size(); }
    std::filesystem::path ManifestListPath(size_t i) { return table_metadata->snapshots[i]->manifest_list_location; }
  };

  struct ManifestList {
    std::vector<iceberg::ManifestFile> manifests;

    std::filesystem::path ManifestPath(size_t i) const { return manifests[i].path; }
  };

  struct Manifest {
    std::vector<iceberg::ManifestEntry> files;
  };

  explicit MetadataTree(const std::filesystem::path& path);

  std::map<int64_t, std::string> MetadataLog() const;

  void FixLocation(const StringFix& fix_meta, const StringFix& fix_data,
                   std::unordered_map<std::string, std::string>& renames,
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

  std::filesystem::path FilesPath() const { return medatada_file_path.parent_path(); }

  friend std::ostream& operator<<(std::ostream& os, const MetadataTree& meta_tree) {
    meta_tree.Print(os);
    return os;
  }
};

void FixLocation(MetadataTree& meta_tree, const std::filesystem::path& metadata_path, const StringFix& fix_meta,
                 const StringFix& fix_data, std::vector<MetadataTree>& prev_meta,
                 std::unordered_map<std::string, std::string>& renames,
                 std::unordered_map<std::string, std::string>& rename_locations);

}  // namespace iceberg::tools
