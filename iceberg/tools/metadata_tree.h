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

    std::filesystem::path DataFilePath(size_t i) const { return files[i].data_file.file_path; }
  };

  explicit MetadataTree(const std::filesystem::path& path);

  std::map<int64_t, std::string> MetadataLog() const;

  void FixLocation(const StringFix& fix_meta, const StringFix& fix_data,
                   std::unordered_map<std::string, std::string>& renames);

  MetadataFile& GetMetadataFile() { return medatada_file; }
  const MetadataFile& GetMetadataFile() const { return medatada_file; }
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

MetadataTree FixLocation(const std::filesystem::path& metadata_path, const StringFix& fix_meta,
                         const StringFix& fix_data, std::vector<MetadataTree>& prev_meta,
                         std::unordered_map<std::string, std::string>& renames, bool strict = false);

}  // namespace iceberg::tools
