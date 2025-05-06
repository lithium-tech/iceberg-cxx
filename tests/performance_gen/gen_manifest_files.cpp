#include <absl/flags/flag.h>
#include <absl/flags/parse.h>

#include <fstream>
#include <iostream>

#include "avro/Writer.hh"
#include "iceberg/manifest_file.h"

ABSL_FLAG(std::string, path, ".",
          "the result file will be written to: "
          "path/warehouse/performance/manifest_files/metadata/"
          "snap-5231658854638766100-1-7e6e13cb-31fd-4de7-8811-02ce7cec44a9.avro");

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);
  const std::string path = absl::GetFlag(FLAGS_path);

  iceberg::ManifestFile manifest_file{
      .added_files_count = 6,
      .added_rows_count = 10000,
      .content = iceberg::ManifestContent::kData,
      .deleted_files_count = 0,
      .deleted_rows_count = 0,
      .existing_files_count = 0,
      .existing_rows_count = 0,
      .length = 6825,
      .min_sequence_number = 2,
      .partition_spec_id = 0,
      .path = "s3://warehouse/performance/manifest_files/metadata/7e6e13cb-31fd-4de7-8811-02ce7cec44a9-m0.avro",
      .sequence_number = 2,
      .snapshot_id = 5231658854638766100};  // example from other tests
  std::vector<iceberg::ManifestFile> manifest_list(10000, manifest_file);
  std::cout << path +
                   "/warehouse/performance/manifest_files/metadata/"
                   "snap-5231658854638766100-1-7e6e13cb-31fd-4de7-8811-02ce7cec44a9.avro"
            << std::endl;
  ;
  std::ofstream file(path +
                     "/warehouse/performance/manifest_files/metadata/"
                     "snap-5231658854638766100-1-7e6e13cb-31fd-4de7-8811-02ce7cec44a9.avro");
  if (!file) {
    std::cerr << "Can't open output file\n";
    return 1;
  }
  file << iceberg::ice_tea::WriteManifestList(manifest_list);
}
