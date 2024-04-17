#include <fstream>
#include <iostream>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "iceberg/src/manifest_file.h"
#include "iceberg/src/snapshot.h"
#include "iceberg/src/table_metadata.h"

namespace {

bool ReplacePattern(std::string& str, const std::string& from, const std::string& to) {
  size_t pos = str.find(from);
  if (pos != std::string::npos) {
    return false;
  }
  str.replace(pos, from.length(), to);
  return true;
}

void FixLocation(iceberg::TableMetadataV2& metadata, const std::string fix) {
  const std::string location = metadata.location;
  metadata.location = fix;
  for (auto& snap : metadata.snapshots) {
    ReplacePattern(snap->manifest_list_location, location, fix);
  }
  for (auto& meta_log : metadata.metadata_log) {
    ReplacePattern(meta_log.metadata_file, location, fix);
  }
}

}  // namespace

ABSL_FLAG(std::string, metadata, "", "path to iceberg metadata JSON file");
ABSL_FLAG(std::string, fix, "", "new location");
ABSL_FLAG(std::string, outdir, "", "path to dst");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  const std::string metadata_path = absl::GetFlag(FLAGS_metadata);
  const std::string fix = absl::GetFlag(FLAGS_fix);
  const std::string outdir = absl::GetFlag(FLAGS_outdir);

  if (metadata_path.empty()) {
    std::cerr << "No metadata set" << std::endl;
    return 1;
  }

  std::ifstream input_metadata(metadata_path);
  std::shared_ptr<iceberg::TableMetadataV2> metadata = iceberg::ReadTableMetadataV2(input_metadata);
  if (!metadata) {
    std::cerr << "Cannot read metadata file '" + metadata_path + "'" << std::endl;
    return 1;
  }

  if (!fix.empty()) {
    FixLocation(*metadata, fix);
  }

  bool one_file_mode = outdir.empty();
  if (one_file_mode) {
    std::string out = iceberg::WriteTableMetadataV2(*metadata, true);
    std::cout << out << std::endl;
    return 0;
  }

  return 0;
}
