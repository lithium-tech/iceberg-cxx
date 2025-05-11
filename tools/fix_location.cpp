#include <absl/flags/flag.h>
#include <absl/flags/parse.h>

#include <filesystem>
#include <iostream>

#include "tools/metadata_tree.h"

ABSL_FLAG(std::string, metadata, "", "path to iceberg metadata JSON file");
ABSL_FLAG(std::string, old, "", "old location");
ABSL_FLAG(std::string, fix, "", "new location");
ABSL_FLAG(std::string, outdir, "", "path to dst");
ABSL_FLAG(bool, clean, true, "clean outdir");

using iceberg::tools::MetadataTree;
using iceberg::tools::StringFix;

int main(int argc, char** argv) {
  try {
    absl::ParseCommandLine(argc, argv);

    const std::filesystem::path metadata_path = absl::GetFlag(FLAGS_metadata);
    const std::string old = absl::GetFlag(FLAGS_old);
    const std::string fix = absl::GetFlag(FLAGS_fix);
    const std::filesystem::path outdir = absl::GetFlag(FLAGS_outdir);
    const bool clean = absl::GetFlag(FLAGS_clean);

    if (metadata_path.empty()) {
      throw std::runtime_error("No metadata set");
    }

    std::vector<MetadataTree> prev_meta;
    MetadataTree meta_tree(metadata_path);
    iceberg::tools::LoadTree(meta_tree, metadata_path, prev_meta, false);

    StringFix fix_paths{old, fix};
    if (fix_paths.NeedFix()) {
      std::unordered_map<std::string, std::string> renames_data;
      std::unordered_map<std::string, std::string> renames_meta;
      std::unordered_map<std::string, std::string> renames_locations;

      for (auto& prev_tree : prev_meta) {
        prev_tree.FixLocation(fix_paths, renames_data, renames_meta, renames_locations);
      }
      meta_tree.FixLocation(fix_paths, renames_data, renames_meta, renames_locations);
    }

    for (auto& prev_tree : prev_meta) {
      std::cout << prev_tree << std::endl;
    }
    std::cout << meta_tree << std::endl;

    if (!outdir.empty()) {
      if (std::filesystem::exists(outdir)) {
        if (!std::filesystem::is_directory(outdir)) {
          throw std::runtime_error(outdir.string() + " is not a direcotry");
        } else if (clean) {
          std::filesystem::remove_all(outdir);
        }
      }
      if (!std::filesystem::exists(outdir)) {
        if (!std::filesystem::create_directories(outdir)) {
          throw std::runtime_error("cannot create direcotry " + outdir.string());
        }
      }

      for (auto& prev_tree : prev_meta) {
        prev_tree.WriteFiles(outdir);
      }
      meta_tree.WriteFiles(outdir);
    }
  } catch (std::exception& ex) {
    std::cerr << ex.what() << std::endl;
    return 1;
  }

  return 0;
}
