#include <absl/flags/flag.h>
#include <absl/flags/parse.h>

#include <fstream>
#include <iostream>

#include "arrow/io/file.h"
#include "arrow/result.h"
#include "iceberg/common/fs/filesystem_wrapper.h"
#include "iceberg/tea_scan.h"

namespace {

using namespace iceberg;
struct Metrics {
  uint64_t requests = 0;
  uint64_t bytes_read = 0;
  uint64_t files_opened = 0;
};

class LoggingInputFile : public InputFileWrapper {
 public:
  LoggingInputFile(std::shared_ptr<arrow::io::RandomAccessFile> file, std::shared_ptr<Metrics> metrics)
      : InputFileWrapper(file), metrics_(metrics) {}

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::ReadAt(position, nbytes, out);
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes, out);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes);
  }

 private:
  void TakeRequestIntoAccount(int64_t bytes) {
    ++metrics_->requests;
    metrics_->bytes_read += bytes;
  }

  std::shared_ptr<Metrics> metrics_;
};

class LoggingFileSystem : public FileSystemWrapper {
 public:
  LoggingFileSystem(std::shared_ptr<arrow::fs::FileSystem> fs, std::shared_ptr<Metrics> metrics)
      : FileSystemWrapper(fs), metrics_(metrics) {}

  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(const std::string& path) override {
    ++metrics_->files_opened;
    ARROW_ASSIGN_OR_RAISE(auto file, FileSystemWrapper::OpenInputFile(path));
    return std::make_shared<LoggingInputFile>(file, metrics_);
  }

 private:
  std::shared_ptr<Metrics> metrics_;
};

void run_manifest_entries(std::shared_ptr<arrow::fs::FileSystem> fs, const int times,
                          const iceberg::ice_tea::GetScanMetadataConfig& config) {
  for (int _ = 0; _ < times; ++_) {
    auto maybe_scan_metadata =
        iceberg::ice_tea::GetScanMetadata(fs,
                                          "s3://warehouse/performance/manifest_entries/metadata/"
                                          "00001-3e9c137d-3f3a-4d9f-ad3c-abd4911f4c9f.metadata.json",
                                          config);
    if (maybe_scan_metadata.status() != arrow::Status::OK()) {
      std::cerr << "run_manifest_entries failed" << std::endl;
      exit(1);
    }
  }
}

void run_manifest_entries_wide(std::shared_ptr<arrow::fs::FileSystem> fs, const int times,
                               const iceberg::ice_tea::GetScanMetadataConfig& config) {
  for (int _ = 0; _ < times; ++_) {
    auto maybe_scan_metadata =
        iceberg::ice_tea::GetScanMetadata(fs,
                                          "s3://warehouse/performance/manifest_entries_wide/metadata/"
                                          "00001-95d2fd1a-501a-4db4-a6b2-49c2d1a87e71.metadata.json",
                                          config);
    if (maybe_scan_metadata.status() != arrow::Status::OK()) {
      std::cerr << "run_manifest_entries_wide failed" << std::endl;
      exit(1);
    }
  }
}

void run_manifest_files(std::shared_ptr<arrow::fs::FileSystem> fs, const int times,
                        const iceberg::ice_tea::GetScanMetadataConfig& config) {
  for (int _ = 0; _ < times; ++_) {
    auto maybe_scan_metadata = iceberg::ice_tea::GetScanMetadata(
        fs,
        "s3://warehouse/performance/manifest_files/metadata/00002-37c508a5-8a06-4823-845e-889dff066f72.metadata.json",
        config);
    if (maybe_scan_metadata.status() != arrow::Status::OK()) {
      std::cerr << "run_manifest_files failed" << std::endl;
      exit(1);
    }
  }
}

void run_schemas(std::shared_ptr<arrow::fs::FileSystem> fs, const int times,
                 const iceberg::ice_tea::GetScanMetadataConfig& config) {
  for (int _ = 0; _ < times; ++_) {
    auto maybe_scan_metadata = iceberg::ice_tea::GetScanMetadata(
        fs, "s3://warehouse/performance/schemas/metadata/00868-7a2e9a74-46be-4dd4-a81a-771445e15034.metadata.json",
        config);
    if (maybe_scan_metadata.status() != arrow::Status::OK()) {
      std::cerr << "run_schemas failed" << std::endl;
      exit(1);
    }
  }
}

void run_snapshots(std::shared_ptr<arrow::fs::FileSystem> fs, const int times,
                   const iceberg::ice_tea::GetScanMetadataConfig& config) {
  for (int _ = 0; _ < times; ++_) {
    auto maybe_scan_metadata = iceberg::ice_tea::GetScanMetadata(
        fs, "s3://warehouse/performance/snapshots/metadata/01999-084000f2-7dd9-4e7c-adfd-24ec13d717c0.metadata.json",
        config);
    if (maybe_scan_metadata.status() != arrow::Status::OK()) {
      std::cerr << "run_snapshots failed" << std::endl;
      exit(1);
    }
  }
}
}  // namespace

ABSL_FLAG(int, manifest_entries, 0, "how many times manifest entries performance test will run");
ABSL_FLAG(int, manifest_entries_wide, 0, "how many times manifest entries performance test will run");
ABSL_FLAG(int, manifest_files, 0, "how many times manifest files performance test will run");
ABSL_FLAG(int, schemas, 0, "how many times schemas performance test will run");
ABSL_FLAG(int, snapshots, 0, "how many times snapshots performance test will run");
ABSL_FLAG(bool, full, true, "if true, GetScanMetadata claims all metadata");

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);
  const int manifest_entries = absl::GetFlag(FLAGS_manifest_entries);
  const int manifest_entries_wide = absl::GetFlag(FLAGS_manifest_entries_wide);
  const int manifest_files = absl::GetFlag(FLAGS_manifest_files);
  const int schemas = absl::GetFlag(FLAGS_schemas);
  const int snapshots = absl::GetFlag(FLAGS_snapshots);
  const bool full = absl::GetFlag(FLAGS_full);

  std::shared_ptr<Metrics> metrics = std::make_shared<Metrics>(Metrics{0, 0, 0});

  std::shared_ptr<arrow::fs::FileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();
  fs = std::make_shared<LoggingFileSystem>(fs, metrics);

  iceberg::ice_tea::GetScanMetadataConfig config;

  if (!full) {
    config.manifest_entry_deserializer_config.datafile_config.extract_column_sizes = false;
    config.manifest_entry_deserializer_config.datafile_config.extract_value_counts = false;
    config.manifest_entry_deserializer_config.datafile_config.extract_null_value_counts = false;
    config.manifest_entry_deserializer_config.datafile_config.extract_nan_value_counts = false;
    config.manifest_entry_deserializer_config.datafile_config.extract_distinct_counts = false;
  }

  run_manifest_entries(fs, manifest_entries, config);
  run_manifest_entries_wide(fs, manifest_entries_wide, config);
  run_manifest_files(fs, manifest_files, config);
  run_schemas(fs, schemas, config);
  run_snapshots(fs, snapshots, config);
  std::cerr << "FileSystem report: requests: " << metrics->requests << std::endl;
  std::cerr << "FileSystem report: bytes read: " << metrics->bytes_read << std::endl;
  std::cerr << "FileSystem report: files opened: " << metrics->files_opened << std::endl;
  return 0;
}
