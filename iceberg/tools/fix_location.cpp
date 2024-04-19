#include <filesystem>
#include <fstream>
#include <iostream>
#include <unordered_map>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "iceberg/src/manifest_entry.h"
#include "iceberg/src/manifest_file.h"
#include "iceberg/src/snapshot.h"
#include "iceberg/src/table_metadata.h"

namespace {

bool ReplacePattern(std::string& str, const std::string& from, const std::string& to) {
  size_t pos = str.find(from);
  if (pos == std::string::npos) {
    return false;
  }
  str.replace(pos, from.length(), to);
  return true;
}

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

  explicit MetadataTree(const std::filesystem::path& path) : medatada_file_path(std::filesystem::absolute(path)) {
    std::ifstream input_metadata(medatada_file_path);
    medatada_file.table_metadata = iceberg::ice_tea::ReadTableMetadataV2(input_metadata);
    if (!medatada_file.table_metadata) {
      throw std::runtime_error("Cannot read metadata file '" + medatada_file_path.string() + "'");
    }

    for (size_t i = 0; i < medatada_file.ListsCount(); ++i) {
      auto list_path = medatada_file.ManifestListPath(i);

      std::ifstream list_input(FilesPath() / list_path.filename());
      auto list =
          std::make_shared<ManifestList>(ManifestList{.manifests = iceberg::ice_tea::ReadManifestList(list_input)});

      if (!manifests_lists.contains(list_path.filename())) {
        for (auto& man_file : list->manifests) {
          std::filesystem::path man_path = man_file.path;

          if (!manifests.contains(man_path.filename())) {
            std::ifstream list_input(FilesPath() / man_path.filename());
            auto man = std::make_shared<Manifest>(Manifest{.files = iceberg::ice_tea::ReadManifestEntries(list_input)});

            manifests.emplace(man_path.filename(), std::move(man));
          }
        }

        manifests_lists.emplace(list_path.filename(), list);
      }
    }
  }

  void FixLocation(const std::string& new_location) {
    auto& metadata = medatada_file.table_metadata;
    const std::string old_location = metadata->location;
    metadata->location = new_location;

    for (auto& snap : metadata->snapshots) {
      ReplacePattern(snap->manifest_list_location, old_location, new_location);
    }
    for (auto& meta_log : metadata->metadata_log) {
      ReplacePattern(meta_log.metadata_file, old_location, new_location);

      // TODO(chertus): fix old snapshots?
    }

    for (auto& [_, man_list] : manifests_lists) {
      for (auto& man : man_list->manifests) {
        ReplacePattern(man.path, old_location, new_location);
      }
    }

    for (auto& [_, man] : manifests) {
      for (auto& file : man->files) {
        ReplacePattern(file.data_file.file_path, old_location, new_location);
      }
    }
  }

  std::string SerializeMetadataFile() const {
    return iceberg::ice_tea::WriteTableMetadataV2(*medatada_file.table_metadata, true);
  }

 private:
  std::filesystem::path medatada_file_path;
  MetadataFile medatada_file;
  std::unordered_map<std::string, std::shared_ptr<ManifestList>> manifests_lists;
  std::unordered_map<std::string, std::shared_ptr<Manifest>> manifests;

  std::filesystem::path FilesPath() const { return medatada_file_path.parent_path(); }
};

std::ostream& operator<<(std::ostream& os, const MetadataTree& meta_tree) {
  os << meta_tree.SerializeMetadataFile() << std::endl;
  return os;
}

}  // namespace

ABSL_FLAG(std::string, metadata, "", "path to iceberg metadata JSON file");
ABSL_FLAG(std::string, fix, "", "new location");
ABSL_FLAG(std::string, outdir, "", "path to dst");

int main(int argc, char** argv) {
  try {
    absl::ParseCommandLine(argc, argv);

    const std::filesystem::path metadata_path = absl::GetFlag(FLAGS_metadata);
    const std::string fix = absl::GetFlag(FLAGS_fix);
    const std::filesystem::path outdir = absl::GetFlag(FLAGS_outdir);

    if (metadata_path.empty()) {
      std::cerr << "No metadata set" << std::endl;
      return 1;
    }

    MetadataTree meta_tree(metadata_path);

    std::unordered_map<std::string, iceberg::ManifestFile> manifests;
    if (!fix.empty()) {
      meta_tree.FixLocation(fix);
    }

    std::cout << meta_tree << std::endl;
  } catch (std::exception& ex) {
    std::cerr << ex.what() << std::endl;
    return 1;
  }

  return 0;
}
