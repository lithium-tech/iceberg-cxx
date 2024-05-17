#include <filesystem>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "iceberg/tools/metadata_tree.h"

ABSL_FLAG(std::string, metadata, "", "path to iceberg metadata JSON file");
ABSL_FLAG(std::string, old, "", "old location");
ABSL_FLAG(std::string, fix, "", "new location");
ABSL_FLAG(std::string, outdir, "", "path to dst");
ABSL_FLAG(int, strict, 0, "fail on errors in previous snapshots");

using iceberg::tools::MetadataTree;
using iceberg::tools::StringFix;

int main(int argc, char** argv) {
  try {
    absl::ParseCommandLine(argc, argv);

    const std::filesystem::path metadata_path = absl::GetFlag(FLAGS_metadata);
    const std::string old = absl::GetFlag(FLAGS_old);
    const std::string fix = absl::GetFlag(FLAGS_fix);
    const std::filesystem::path outdir = absl::GetFlag(FLAGS_outdir);
    const int strict = absl::GetFlag(FLAGS_strict);

    if (metadata_path.empty()) {
      std::cerr << "No metadata set" << std::endl;
      return 1;
    }

    StringFix fix_meta{old, fix};
    StringFix fix_data{old, fix};
    std::vector<MetadataTree> prev_meta;
    std::unordered_map<std::string, std::string> renames;
    MetadataTree meta_tree = iceberg::tools::FixLocation(metadata_path, fix_meta, fix_data, prev_meta, renames, strict);

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
