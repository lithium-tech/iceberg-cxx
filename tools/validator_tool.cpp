#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <fstream>
#include <iostream>

#include "arrow/filesystem/s3fs.h"
#include "iceberg/tea_scan.h"
#include "tools/validation.h"

ABSL_FLAG(std::string, restrictions, "", "path to restrictions file");
ABSL_FLAG(bool, expect_all_params, false, "if true expect all parameters in restrictions file");
ABSL_FLAG(std::string, mode, "", "table_metadata/manifests");
ABSL_FLAG(std::string, filesystem, "local", "filesystem to use (local or s3)");
ABSL_FLAG(std::string, access_key_id, "", "s3 access key");
ABSL_FLAG(std::string, secret_key, "", "s3 secret key");
ABSL_FLAG(std::string, s3_endpoint, "", "s3 endpoint");
ABSL_FLAG(std::vector<std::string>, paths, {}, "paths to files to be validated");

using namespace iceberg::tools;

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);
  const std::string restrictions = absl::GetFlag(FLAGS_restrictions);
  const bool expect_all_params = absl::GetFlag(FLAGS_expect_all_params);
  const std::string mode = absl::GetFlag(FLAGS_mode);
  const std::string filesystem_type = absl::GetFlag(FLAGS_filesystem);
  const std::string access_key_id = absl::GetFlag(FLAGS_access_key_id);
  const std::string secret_key = absl::GetFlag(FLAGS_secret_key);
  std::vector<std::string> paths = absl::GetFlag(FLAGS_paths);
  if (restrictions.empty()) {
    std::cerr << "--restrictions is required" << std::endl;
    return 1;
  }
  if (mode.empty()) {
    std::cerr << "--mode is required" << std::endl;
    return 1;
  }
  if (paths.empty()) {
    std::cerr << "--paths is required" << std::endl;
    return 1;
  }
  std::shared_ptr<arrow::fs::FileSystem> fs;
  if (filesystem_type == "local") {
    fs = std::make_shared<arrow::fs::LocalFileSystem>();
  } else if (filesystem_type == "s3") {
    arrow::fs::InitializeS3(arrow::fs::S3GlobalOptions{}).ok();
    const std::string access_key = absl::GetFlag(FLAGS_access_key_id);
    const std::string secret_key = absl::GetFlag(FLAGS_secret_key);
    auto s3options = arrow::fs::S3Options::FromAccessKey(access_key, secret_key);
    s3options.endpoint_override = absl::GetFlag(FLAGS_s3_endpoint);
    s3options.scheme = "http";
    auto maybe_fs = arrow::fs::S3FileSystem::Make(s3options);
    if (!maybe_fs.ok()) {
      std::cerr << maybe_fs.status() << std::endl;
      return 1;
    }
    fs = maybe_fs.MoveValueUnsafe();
  } else {
    std::cerr << "Unexpected filesystem type" << std::endl;
    return 1;
  }
  IcebergMetadataValidator validator;
  try {
    if (mode == "manifests") {
      validator.SetRestrictionsManifests(RestrictionsManifests::Read(restrictions, expect_all_params));
      std::vector<iceberg::Manifest> manifests;
      for (const auto& path : paths) {
        auto maybe_content = iceberg::ice_tea::ReadFile(fs, path);
        if (!maybe_content.ok()) {
          std::cerr << maybe_content.status() << std::endl;
          throw std::runtime_error("");
        }
        auto content = maybe_content.MoveValueUnsafe();
        manifests.push_back(iceberg::ice_tea::ReadManifestEntries(content, {}));
      }
      validator.ValidateManifests(manifests);
    } else if (mode == "table_metadata") {
      validator.SetRestrictionsTableMetadata(RestrictionsTableMetadata::Read(restrictions, expect_all_params));
      for (const auto& path : paths) {
        auto maybe_content = iceberg::ice_tea::ReadFile(fs, path);
        if (!maybe_content.ok()) {
          std::cerr << maybe_content.status() << std::endl;
          throw std::runtime_error("");
        }
        auto content = maybe_content.MoveValueUnsafe();
        auto meta = iceberg::ice_tea::ReadTableMetadataV2(content);
        if (!meta) {
          throw std::runtime_error("Failed to read meta from iceberg");
        }
        validator.ValidateTableMetadata(meta);
      }
    } else {
      throw std::runtime_error("Unexpected mode");
    }
  } catch (const std::exception& e) {
    if (arrow::fs::IsS3Initialized() && !arrow::fs::IsS3Finalized()) {
      arrow::fs::EnsureS3Finalized().ok();
    }
    std::cerr << e.what() << std::endl;
    return 1;
  }
  if (arrow::fs::IsS3Initialized() && !arrow::fs::IsS3Finalized()) {
    arrow::fs::EnsureS3Finalized().ok();
  }
  return 0;
}
