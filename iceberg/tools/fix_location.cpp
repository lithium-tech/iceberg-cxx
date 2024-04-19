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

std::string RemoveSuffix(const std::string& str, const std::string& suffix) {
  if (suffix.empty()) {
    return str;
  }
  if (str.ends_with(suffix)) {
    return str.substr(0, str.size() - suffix.size());
  }
  return str;
}

std::string FileType(iceberg::ContentFile::FileContent content) {
  switch (content) {
    case iceberg::ContentFile::FileContent::kData:
      return "data";
    case iceberg::ContentFile::FileContent::kPositionDeletes:
      return "del(pos)";
    case iceberg::ContentFile::FileContent::kEqualityDeletes:
      return "del(equal)";
  }
  return "unknown";
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

  std::map<int64_t, std::string> MetadataLog() const {
    std::map<int64_t, std::string> out;
    for (auto& log : medatada_file.table_metadata->metadata_log) {
      out.emplace(log.timestamp_ms, log.metadata_file);
    }
    return out;
  }

  void FixLocation(const std::string& new_location, const std::string& suffix) {
    auto& metadata = medatada_file.table_metadata;
    const std::string old_location = RemoveSuffix(metadata->location, suffix);
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

  void WriteFiles(const std::filesystem::path& out_dir) const {
    {
      auto out_path = out_dir / medatada_file_path.filename();
      std::string serialized = SerializeMetadataFile();
      std::ofstream ofstream(out_path);
      ofstream.write(serialized.data(), serialized.size());
    }

    for (auto& [path, man_list] : manifests_lists) {
      auto out_path = out_dir / std::filesystem::path(path).filename();
      std::string serialized = iceberg::ice_tea::WriteManifestList(man_list->manifests);
      std::ofstream ofstream(out_path);
      ofstream.write(serialized.data(), serialized.size());
    }

    for (auto& [path, man] : manifests) {
      auto out_path = out_dir / std::filesystem::path(path).filename();
      std::string serialized = iceberg::ice_tea::WriteManifestEntries(man->files);
      std::ofstream ofstream(out_path);
      ofstream.write(serialized.data(), serialized.size());
    }
  }

 private:
  std::filesystem::path medatada_file_path;
  MetadataFile medatada_file;
  std::unordered_map<std::string, std::shared_ptr<ManifestList>> manifests_lists;
  std::unordered_map<std::string, std::shared_ptr<Manifest>> manifests;

  std::filesystem::path FilesPath() const { return medatada_file_path.parent_path(); }

  friend std::ostream& operator<<(std::ostream& os, const MetadataTree& meta_tree) {
    os << "metadata " << meta_tree.medatada_file.table_metadata->location << "/" << meta_tree.medatada_file_path
       << std::endl;
#if 0
    os << meta_tree.SerializeMetadataFile() << std::endl;
#endif
    for (auto& snap : meta_tree.medatada_file.table_metadata->snapshots) {
      os << "snapshot seqno " << snap->sequence_number << " id " << snap->snapshot_id << " "
         << snap->manifest_list_location << std::endl;
    }
    for (auto& [path, man_list] : meta_tree.manifests_lists) {
      os << path << " [";
      for (auto& man : man_list->manifests) {
        os << man.path << ", ";
      }
      os << "]" << std::endl;
    }
    for (auto& [path, man] : meta_tree.manifests) {
      for (auto& entry : man->files) {
        os << FileType(entry.data_file.content) << " " << entry.data_file.file_path << std::endl;
      }
    }
    return os;
  }
};

}  // namespace

ABSL_FLAG(std::string, metadata, "", "path to iceberg metadata JSON file");
ABSL_FLAG(std::string, fix, "", "new location");
ABSL_FLAG(std::string, suffix, "/iceberg", "suffix in location to keep");
ABSL_FLAG(std::string, outdir, "", "path to dst");

int main(int argc, char** argv) {
  try {
    absl::ParseCommandLine(argc, argv);

    const std::filesystem::path metadata_path = absl::GetFlag(FLAGS_metadata);
    const std::string fix = absl::GetFlag(FLAGS_fix);
    const std::string suffix = absl::GetFlag(FLAGS_suffix);
    const std::filesystem::path outdir = absl::GetFlag(FLAGS_outdir);

    if (metadata_path.empty()) {
      std::cerr << "No metadata set" << std::endl;
      return 1;
    }

    MetadataTree meta_tree(metadata_path);
    auto meta_log = meta_tree.MetadataLog();
    std::vector<MetadataTree> prev_meta;
    prev_meta.reserve(meta_log.size());
    for (auto& [_, meta_file_path] : meta_log) {
      auto path =
          std::filesystem::absolute(metadata_path).parent_path() / std::filesystem::path(meta_file_path).filename();
      prev_meta.emplace_back(MetadataTree(path));
    }

    if (!fix.empty()) {
      for (auto& prev_tree : prev_meta) {
        prev_tree.FixLocation(fix, suffix);
      }
      meta_tree.FixLocation(fix, suffix);
    }

    if (!outdir.empty()) {
      for (auto& prev_tree : prev_meta) {
        prev_tree.WriteFiles(outdir);
      }
      meta_tree.WriteFiles(outdir);
    }

    for (auto& prev_tree : prev_meta) {
      std::cout << prev_tree << std::endl;
    }
    std::cout << meta_tree << std::endl;
  } catch (std::exception& ex) {
    std::cerr << ex.what() << std::endl;
    return 1;
  }

  return 0;
}
