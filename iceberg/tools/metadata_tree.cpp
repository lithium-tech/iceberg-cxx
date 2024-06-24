#include "iceberg/tools/metadata_tree.h"

#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <utility>

namespace iceberg::tools {

namespace {

bool ReplacePattern(std::string& str, const std::string& from, const std::string& to) {
  size_t pos = str.find(from);
  if (pos == std::string::npos) {
    return false;
  }
  str.replace(pos, from.length(), to);
  return true;
}

bool FixString(std::string& str, const StringFix& fix, std::unordered_map<std::string, std::string>& renames) {
  std::string old_str = str;
  if (ReplacePattern(str, fix.from, fix.to)) {
    renames[old_str] = str;
    return true;
  }
  return false;
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

}  // namespace

MetadataTree::MetadataTree(const std::filesystem::path& path) : medatada_file_path(std::filesystem::absolute(path)) {
  if (!std::filesystem::exists(medatada_file_path)) {
    throw std::runtime_error("No metadata file '" + medatada_file_path.string() + "'");
  }
  std::ifstream input_metadata(medatada_file_path);
  medatada_file.table_metadata = iceberg::ice_tea::ReadTableMetadataV2(input_metadata);
  if (!medatada_file.table_metadata) {
    throw std::runtime_error("Cannot read metadata file '" + medatada_file_path.string() + "'");
  }

  for (size_t i = 0; i < medatada_file.ListsCount(); ++i) {
    auto list_path = medatada_file.ManifestListPath(i);

    auto list_file_path = FilesPath() / list_path.filename();
    if (!std::filesystem::exists(list_file_path)) {
      throw std::runtime_error("No manifests list file '" + list_file_path.string() + "'");
    }
    std::ifstream list_input(list_file_path);
    auto list =
        std::make_shared<ManifestList>(ManifestList{.manifests = iceberg::ice_tea::ReadManifestList(list_input)});

    if (manifests_lists.try_emplace(list_path.filename(), list).second) {
      for (auto& man_file : list->manifests) {
        std::filesystem::path man_path = man_file.path;

        if (!manifests.contains(man_path.filename())) {
          auto man_file_path = FilesPath() / man_path.filename();
          if (!std::filesystem::exists(man_file_path)) {
            throw std::runtime_error("No manifest file '" + man_file_path.string() + "'");
          }
          std::ifstream list_input(man_file_path);
          auto man = std::make_shared<Manifest>(Manifest{.files = iceberg::ice_tea::ReadManifestEntries(list_input)});

          manifests.emplace(man_path.filename(), std::move(man));
        }
      }
    }
  }
}

std::map<int64_t, std::string> MetadataTree::MetadataLog() const {
  std::map<int64_t, std::string> out;
  for (auto& log : medatada_file.table_metadata->metadata_log) {
    out.emplace(log.timestamp_ms, log.metadata_file);
  }
  return out;
}

void MetadataTree::FixLocation(const StringFix& fix_meta, const StringFix& fix_data,
                               std::unordered_map<std::string, std::string>& renames,
                               std::unordered_map<std::string, std::string>& rename_locations) {
  auto& metadata = medatada_file.table_metadata;
  FixString(metadata->location, fix_meta, rename_locations);

  for (auto& snap : metadata->snapshots) {
    FixString(snap->manifest_list_location, fix_meta, renames);
  }
  for (auto& meta_log : metadata->metadata_log) {
    FixString(meta_log.metadata_file, fix_meta, renames);
  }

  for (auto& [_, man_list] : manifests_lists) {
    for (auto& man : man_list->manifests) {
      FixString(man.path, fix_meta, renames);
    }
  }

  for (auto& [_, man] : manifests) {
    for (auto& file : man->files) {
      FixString(file.data_file.file_path, fix_data, renames);
    }
  }
}

std::string MetadataTree::SerializeMetadataFile() const {
  return iceberg::ice_tea::WriteTableMetadataV2(*medatada_file.table_metadata, true);
}

void MetadataTree::WriteFiles(const std::filesystem::path& out_dir) const {
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

void MetadataTree::Print(std::ostream& os, size_t limit_files) const {
  os << "metadata " << medatada_file_path.filename().string() << std::endl;
#if 0
    os << meta_tree.SerializeMetadataFile() << std::endl;
#endif
  for (auto& snap : medatada_file.table_metadata->snapshots) {
    os << "snapshot " << snap->snapshot_id << " (seqno " << snap->sequence_number << ") "
       << snap->manifest_list_location << std::endl;
  }
  for (auto& [list_filename, man_list] : manifests_lists) {
    os << "manlist " << list_filename << " [";
    for (auto& man : man_list->manifests) {
      os << man.path << ", ";
    }
    os << "]" << std::endl;
  }

  for (auto& [man_filename, man] : manifests) {
    size_t count = limit_files;
    os << "manifest " << man_filename << std::endl;
    for (auto& entry : man->files) {
      os << FileType(entry.data_file.content) << " " << entry.data_file.file_path << std::endl;
      if (limit_files) {
        --count;
        if (!count) {
          os << "... (total: " << man->files.size() << ")" << std::endl;
          break;
        }
      }
    }
  }
}

void FixLocation(MetadataTree& meta_tree, const std::filesystem::path& metadata_path, const StringFix& fix_meta,
                 const StringFix& fix_data, std::vector<MetadataTree>& prev_meta,
                 std::unordered_map<std::string, std::string>& renames,
                 std::unordered_map<std::string, std::string>& rename_locations) {
  auto meta_log = meta_tree.MetadataLog();
  prev_meta.reserve(meta_log.size());
  for (auto& [_, meta_file_path] : meta_log) {
    auto path =
        std::filesystem::absolute(metadata_path).parent_path() / std::filesystem::path(meta_file_path).filename();

    try {
      MetadataTree old_meta(path);
      prev_meta.emplace_back(std::move(old_meta));
    } catch (std::exception& ex) {
      std::cerr << "Error while processing " << path << ": " << ex.what() << std::endl;
    }
  }

  if (!fix_meta.NeedFix() && !fix_data.NeedFix()) {
    return;
  }

  for (auto& prev_tree : prev_meta) {
    prev_tree.FixLocation(fix_meta, fix_data, renames, rename_locations);
  }
  meta_tree.FixLocation(fix_meta, fix_data, renames, rename_locations);
}

}  // namespace iceberg::tools
