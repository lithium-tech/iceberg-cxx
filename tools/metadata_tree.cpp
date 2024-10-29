#include "tools/metadata_tree.h"

#include <exception>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "iceberg/write.h"

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

void MetadataTree::MetadataFile::RemoveOtherSnapshots(int64_t snapshot_id, bool switch_main) {
  std::shared_ptr<Snapshot> snapshot;
  for (auto& snap : table_metadata->snapshots) {
    if (snap->snapshot_id == snapshot_id) {
      snapshot = snap;
      break;
    }
  }
  if (!snapshot) {
    throw std::runtime_error("no snapshot " + std::to_string(snapshot_id));
  }
  if (!snapshot->schema_id) {
    throw std::runtime_error("snapshot " + std::to_string(snapshot_id) + " has no schema_id");
  }

  table_metadata->current_snapshot_id = snapshot_id;
  table_metadata->snapshots = {snapshot};
  table_metadata->current_schema_id = *snapshot->schema_id;

  bool schema_found = false;
  for (auto& schema : table_metadata->schemas) {
    if (table_metadata->current_schema_id == schema->SchemaId()) {
      table_metadata->schemas = std::vector<std::shared_ptr<Schema>>{schema};
      schema_found = true;
      break;
    }
  }
  if (!schema_found) {
    throw std::runtime_error("no schema " + std::to_string(table_metadata->current_schema_id) +
                             " needed for shapshot " + std::to_string(snapshot_id));
  }

  for (auto& log : table_metadata->snapshot_log) {
    if (log.snapshot_id == snapshot_id) {
      table_metadata->snapshot_log = {log};
      break;
    }
  }
  std::map<std::string, SnapshotRef> refs;
  for (auto& [name, ref] : table_metadata->refs) {
    if (ref.snapshot_id == snapshot_id) {
      refs.emplace(std::move(name), std::move(ref));
    }
  }
  if (switch_main) {
    refs.emplace("main", SnapshotRef{snapshot_id, "branch"});
  }
  table_metadata->refs = std::move(refs);
}

MetadataTree::MetadataTree(const std::filesystem::path& path, bool ignore_missing_snapshots)
    : medatada_file_path(std::filesystem::absolute(path)) {
  medatada_file = ReadMetadataFile(medatada_file_path);

  auto files_path = medatada_file_path.parent_path();  // actual local files location

  std::vector<std::shared_ptr<Snapshot>> actual_snaps;
  actual_snaps.reserve(medatada_file.Snapshots().size());
  for (auto& snap : medatada_file.Snapshots()) {
    if (AddSnapshot(snap, files_path, ignore_missing_snapshots)) {
      actual_snaps.emplace_back(snap);
    }
  }
  medatada_file.table_metadata->snapshots = std::move(actual_snaps);
}

MetadataTree::MetadataTree(const std::filesystem::path& path, int64_t snapshot_id)
    : medatada_file_path(std::filesystem::absolute(path)) {
  medatada_file = ReadMetadataFile(medatada_file_path);

  auto files_path = medatada_file_path.parent_path();  // actual local files location
  medatada_file.RemoveOtherSnapshots(snapshot_id);
  AddSnapshot(medatada_file.Snapshots()[0], files_path, false);
}

MetadataTree::MetadataTree(const std::filesystem::path& path, const std::string& ref)
    : medatada_file_path(std::filesystem::absolute(path)) {
  medatada_file = ReadMetadataFile(medatada_file_path);

  auto& known_refs = medatada_file.Refs();
  auto it = known_refs.find(ref);
  if (it == known_refs.end()) {
    throw std::runtime_error("No ref (branch or tag) '" + ref + "' in '" + path.string() + "'");
  }

  int64_t snapshot_id = it->second.snapshot_id;

  auto files_path = medatada_file_path.parent_path();  // actual local files location
  medatada_file.RemoveOtherSnapshots(snapshot_id);
  AddSnapshot(medatada_file.Snapshots()[0], files_path, false);
}

MetadataTree::MetadataFile MetadataTree::ReadMetadataFile(const std::filesystem::path& path) {
  if (!std::filesystem::exists(path)) {
    throw std::runtime_error("No metadata file '" + path.string() + "'");
  }
  std::ifstream input_metadata(path);
  MetadataFile metadata;
  metadata.table_metadata = iceberg::ice_tea::ReadTableMetadataV2(input_metadata);
  if (!metadata.table_metadata) {
    throw std::runtime_error("Cannot read metadata file '" + path.string() + "'");
  }
  return metadata;
}

bool MetadataTree::AddSnapshot(const std::shared_ptr<Snapshot>& snap, const std::filesystem::path& files_path,
                               bool ignore_missing_snapshots) {
  std::filesystem::path list_path = snap->manifest_list_location;

  auto list_file_path = files_path / list_path.filename();
  if (!std::filesystem::exists(list_file_path)) {
    if (ignore_missing_snapshots) {
      return false;
    }
    throw std::runtime_error("No manifests list file '" + list_file_path.string() + "'");
  }
  std::ifstream list_input(list_file_path);
  auto list = std::make_shared<ManifestList>(iceberg::ice_tea::ReadManifestList(list_input));

  if (manifests_lists.try_emplace(list_path.filename(), list).second) {
    for (auto& man_file : *list) {
      std::filesystem::path man_path = man_file.path;

      if (!manifests.contains(man_path.filename())) {
        auto man_file_path = files_path / man_path.filename();
        if (!std::filesystem::exists(man_file_path)) {
          if (ignore_missing_snapshots) {
            return false;
          }
          throw std::runtime_error("No manifest file '" + man_file_path.string() + "'");
        }
        std::ifstream list_input(man_file_path);
        auto man = std::make_shared<Manifest>(iceberg::ice_tea::ReadManifestEntries(list_input));

        manifests.emplace(man_path.filename(), std::move(man));
      }
    }
  }
  return true;
}

std::map<int64_t, std::string> MetadataTree::MetadataLog() const {
  std::map<int64_t, std::string> out;
  for (auto& log : medatada_file.table_metadata->metadata_log) {
    out.emplace(log.timestamp_ms, log.metadata_file);
  }
  return out;
}

void MetadataTree::FixLocation(const StringFix& fix_paths, std::unordered_map<std::string, std::string>& renames_data,
                               std::unordered_map<std::string, std::string>& renames_meta,
                               std::unordered_map<std::string, std::string>& rename_locations) {
  auto& metadata = medatada_file.table_metadata;
  FixString(metadata->location, fix_paths, rename_locations);

  for (auto& snap : metadata->snapshots) {
    FixString(snap->manifest_list_location, fix_paths, renames_meta);
  }
  for (auto& meta_log : metadata->metadata_log) {
    FixString(meta_log.metadata_file, fix_paths, renames_meta);
  }

  for (auto& [_, man_list] : manifests_lists) {
    for (auto& man : *man_list) {
      FixString(man.path, fix_paths, renames_meta);
    }
  }

  for (auto& [_, man] : manifests) {
    for (auto& file : man->entries) {
      FixString(file.data_file.file_path, fix_paths, renames_data);
    }
  }
}

std::string MetadataTree::SerializeMetadataFile() const {
  return iceberg::ice_tea::WriteTableMetadataV2(*medatada_file.table_metadata, true);
}

void MetadataTree::WriteFiles(const std::filesystem::path& out_dir) const {
  iceberg::ice_tea::WriteMetadataFile(out_dir / medatada_file_path.filename(), medatada_file.table_metadata);

  for (auto& [path, man_list] : manifests_lists) {
    iceberg::ice_tea::WriteManifestList(out_dir / std::filesystem::path(path).filename(), *man_list);
  }

  for (auto& [path, man] : manifests) {
    iceberg::ice_tea::WriteManifest(out_dir / std::filesystem::path(path).filename(), *man);
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
    for (auto& man : *man_list) {
      os << man.path << ", ";
    }
    os << "]" << std::endl;
  }

  for (auto& [man_filename, man] : manifests) {
    size_t count = limit_files;
    os << "manifest " << man_filename << std::endl;
    for (auto& entry : man->entries) {
      os << FileType(entry.data_file.content) << " " << entry.data_file.file_path << std::endl;
      if (limit_files) {
        --count;
        if (!count) {
          os << "... (total: " << man->entries.size() << ")" << std::endl;
          break;
        }
      }
    }
  }
}

void LoadTree(MetadataTree& meta_tree, const std::filesystem::path& metadata_path, std::vector<MetadataTree>& prev_meta,
              std::unordered_map<std::string, std::string>& renames_data,
              std::unordered_map<std::string, std::string>& renames_meta,
              std::unordered_map<std::string, std::string>& rename_locations, bool ignore_missing_snapshots) {
  auto meta_log = meta_tree.MetadataLog();
  prev_meta.reserve(meta_log.size());
  for (auto& [_, meta_file_path] : meta_log) {
    auto path =
        std::filesystem::absolute(metadata_path).parent_path() / std::filesystem::path(meta_file_path).filename();

    try {
      MetadataTree old_meta(path);
      prev_meta.emplace_back(std::move(old_meta));
    } catch (std::exception& ex) {
      if (!ignore_missing_snapshots) {
        throw std::runtime_error("Error while processing '" + path.string() + "': " + ex.what());
      }
    }
  }
}

void LoadTree(MetadataTree& meta_tree, const std::filesystem::path& metadata_path, std::vector<MetadataTree>& prev_meta,
              bool ignore_missing_snapshots) {
  std::unordered_map<std::string, std::string> renames_data;
  std::unordered_map<std::string, std::string> renames_meta;
  std::unordered_map<std::string, std::string> renames_locations;
  LoadTree(meta_tree, metadata_path, prev_meta, renames_data, renames_meta, renames_locations,
           ignore_missing_snapshots);
}

}  // namespace iceberg::tools
