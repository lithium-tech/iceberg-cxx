#pragma once

#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/s3fs.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "iceberg/common/error.h"

namespace iceberg::tools {
struct CopyOptions {
  bool use_threads = false;
  bool no_check_dest = false;
};

struct S3Init {
  explicit S3Init(arrow::fs::S3LogLevel log_level = arrow::fs::S3LogLevel::Error) { InitS3(log_level); }
  ~S3Init() { FinalizeS3(); }

  static arrow::fs::S3LogLevel LogLevel(const std::string& level) {
    using arrow::fs::S3LogLevel;
    if (level == "off") {
      return S3LogLevel::Off;
    } else if (level == "fatal") {
      return S3LogLevel::Fatal;
    } else if (level == "error") {
      return S3LogLevel::Error;
    } else if (level == "warn") {
      return S3LogLevel::Warn;
    } else if (level == "info") {
      return S3LogLevel::Info;
    } else if (level == "debug") {
      return S3LogLevel::Debug;
    } else if (level == "trace") {
      return S3LogLevel::Trace;
    }
    return S3LogLevel::Error;
  }

  static void InitS3(arrow::fs::S3LogLevel log_level = arrow::fs::S3LogLevel::Error) {
    if (!arrow::fs::IsS3Initialized()) {
      arrow::fs::S3GlobalOptions global_options{};
      global_options.log_level = log_level;
      Ensure(arrow::fs::InitializeS3(global_options).ok(), "Cannot Initialize S3");
    }
  }

  static bool FinalizeS3() {
    if (!arrow::fs::IsS3Finalized()) {
      return arrow::fs::FinalizeS3().ok();
    }
    return true;
  }
};

struct S3Access {
  static constexpr const char* ENDPOINT_URL = "ENDPOINT_URL";
  static constexpr const char* DEFAULT_REGION = "DEFAULT_REGION";
  static constexpr const char* ACCESS_KEY_ID = "ACCESS_KEY_ID";
  static constexpr const char* SECRET_ACCESS_KEY = "SECRET_ACCESS_KEY";
  static constexpr const char* AWS_EC2_METADATA_DISABLED = "AWS_EC2_METADATA_DISABLED";

  arrow::fs::S3Options options;

  void LoadEnvOptions(const std::string env_prefix) {
    options = arrow::fs::S3Options::Defaults();  // reqiures InitS3() first
    options.endpoint_override = GetEnvVar(env_prefix + ENDPOINT_URL);
    options.region = GetEnvVar(env_prefix + DEFAULT_REGION, false);
    options.ConfigureAccessKey(GetEnvVar(env_prefix + ACCESS_KEY_ID), GetEnvVar(env_prefix + SECRET_ACCESS_KEY));
  }

  static void ClearEnvOptions(const std::string& env_prefix = "AWS_") {
    setenv(AWS_EC2_METADATA_DISABLED, "true", 1);
    unsetenv((env_prefix + ENDPOINT_URL).c_str());
    unsetenv((env_prefix + DEFAULT_REGION).c_str());
    unsetenv((env_prefix + ACCESS_KEY_ID).c_str());
    unsetenv((env_prefix + SECRET_ACCESS_KEY).c_str());
  }

  static std::string GetEnvVar(const std::string& env, bool required = true) {
    const char* value = getenv(env.c_str());
    if (!value) {
      if (required) {
        throw std::runtime_error("env var not set '" + env + "'");
      } else {
        return "";
      }
    }
    return value;
  }
};

class S3Client {
 public:
  static constexpr int64_t CHUNK_SIZE = 8 * 1024 * 1024;

  S3Client(bool force, arrow::fs::S3LogLevel log_level = arrow::fs::S3LogLevel::Error, int64_t chunk_size = CHUNK_SIZE,
           const std::string& src_env_prefix = "AWS_", const std::string& dst_env_prefix = "DST_",
           const arrow::io::IOContext& io_context = arrow::io::default_io_context(), bool use_local_fs = false)
      : s3init_(log_level), io_context_(io_context), copy_chunk_size_(chunk_size) {
    if (!use_local_fs) {
      src_access_.LoadEnvOptions(src_env_prefix);
      S3Access::ClearEnvOptions("AWS_");

      fs_ = std::make_shared<arrow::fs::LocalFileSystem>();

      auto s3fs_res = arrow::fs::S3FileSystem::Make(src_access_.options);
      Ensure(s3fs_res.ok(), s3fs_res.status().ToString());

      src_s3fs_ = *s3fs_res;

      if (!dst_env_prefix.empty()) {
        dst_access_.LoadEnvOptions(dst_env_prefix);
        s3fs_res = arrow::fs::S3FileSystem::Make(dst_access_.options);
        Ensure(s3fs_res.ok(), s3fs_res.status().ToString());
        dst_s3fs_ = *s3fs_res;
      }
    } else {
      fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
      src_s3fs_ = fs_;
      dst_s3fs_ = fs_;
    }
  }

  arrow::fs::FileLocator MakeFileLocator(std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& name) const {
    arrow::fs::FileLocator file_loc{.filesystem = fs, .path = CropPrefix(name)};
    if (file_loc.path == name) {
      file_loc.filesystem = fs_;
    }
    Ensure(file_loc.filesystem != nullptr, "fs is not set");
    return file_loc;
  }

  arrow::fs::FileLocator SrcFileLocator(const std::string& name) const { return MakeFileLocator(src_s3fs_, name); }
  arrow::fs::FileLocator DstFileLocator(const std::string& name) const { return MakeFileLocator(dst_s3fs_, name); }

  static void MakeDstDirs(const std::vector<arrow::fs::FileLocator>& dst) {
    for (auto& locator : dst) {
      if (dynamic_cast<arrow::fs::LocalFileSystem*>(locator.filesystem.get())) {
        auto dir = std::filesystem::path(locator.path).parent_path();
        if (!std::filesystem::exists(dir)) {
          std::filesystem::create_directories(dir);
        }
      }
    }
  }

  void NamesToLocators(const std::unordered_map<std::string, std::string>& renames,
                       std::vector<arrow::fs::FileLocator>& src, std::vector<arrow::fs::FileLocator>& dst) {
    src.clear();
    dst.clear();
    src.reserve(renames.size());
    dst.reserve(renames.size());

    for (auto& [src_name, dst_name] : renames) {
      src.push_back(SrcFileLocator(src_name));
      dst.push_back(DstFileLocator(dst_name));
    }
  }

  bool CopyFiles(const std::unordered_map<std::string, std::string>& renames, bool use_threads) {
    if (renames.empty()) {
      return true;
    }

    std::vector<arrow::fs::FileLocator> src;
    std::vector<arrow::fs::FileLocator> dst;
    NamesToLocators(renames, src, dst);

    MakeDstDirs(dst);
    return CopyFiles(src, dst, use_threads);
  }

  bool CopyFiles(const std::vector<arrow::fs::FileLocator>& src, const std::vector<arrow::fs::FileLocator>& dst,
                 bool use_threads) {
    auto status = arrow::fs::CopyFiles(src, dst, io_context_, copy_chunk_size_, use_threads);
    Ensure(status.ok(), status.ToString());

    return true;
  }

  bool CopyDir(const std::string& src, const std::string& dst, bool use_threads) {
    auto src_locator = SrcFileLocator(src);
    auto dst_locator = DstFileLocator(dst);

    arrow::fs::FileSelector selector;
    selector.allow_not_found = false;
    selector.recursive = true;
    selector.base_dir = src_locator.path;

    auto status = arrow::fs::CopyFiles(src_locator.filesystem, selector, dst_locator.filesystem, dst_locator.path,
                                       io_context_, copy_chunk_size_, use_threads);
    Ensure(status.ok(), status.ToString());
    return true;
  }

  std::shared_ptr<arrow::fs::FileSystem> GetSrcFileSystem() const { return src_s3fs_; }

  std::shared_ptr<arrow::fs::FileSystem> GetDstFileSystem() const { return dst_s3fs_; }

 private:
  S3Init s3init_;
  S3Access src_access_;
  S3Access dst_access_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;
  std::shared_ptr<arrow::fs::FileSystem> src_s3fs_;
  std::shared_ptr<arrow::fs::FileSystem> dst_s3fs_;
  const arrow::io::IOContext& io_context_;
  const int64_t copy_chunk_size_;

  static std::vector<arrow::fs::FileInfo> GetFileInfos(const std::vector<arrow::fs::FileLocator>& locations) {
    std::vector<arrow::fs::FileInfo> out;
    out.reserve(locations.size());
    for (auto& [fs, path] : locations) {
      arrow::Result<arrow::fs::FileInfo> res = fs->GetFileInfo(path);
      out.emplace_back(res.ok() ? std::move(*res) : arrow::fs::FileInfo());
    }
    return out;
  }

  static std::string CropPrefix(const std::string& src, const std::string& prefix = "://") {
    auto pos = src.find(prefix);
    if (pos != std::string::npos) {
      return src.substr(pos + prefix.size());
    }
    return src;
  }
};

inline bool Rclone(const std::string& cmd, const std::string& arg1, const std::string& arg2) {
  std::string sh_cmd = "rclone " + cmd + " " + arg1 + " " + arg2;
  if (std::system(sh_cmd.data())) {
    return false;
  }
  return true;
}

inline bool CopyFiles(std::shared_ptr<S3Client> s3client, const std::unordered_map<std::string, std::string>& renames,
                      const CopyOptions& opts) {
  if (s3client) {
    if (!s3client->CopyFiles(renames, opts.use_threads)) {
      return false;
    }
  } else {
    std::string flags;
    if (opts.no_check_dest) {
      flags = "--no-check-dest ";
    } else {
      // By default it checks file size and checksum
      // flags = "--ignore-existing --ignore-checksum --ignore-size ";
    }
    for (auto& [src, dst] : renames) {
      if (!Rclone(flags + "copyto", src, dst)) {
        return false;
      }
    }
  }
  return true;
}

inline void CopyFilesOrThrow(std::shared_ptr<S3Client> s3client,
                             const std::unordered_map<std::string, std::string>& renames, const CopyOptions& opts) {
  Ensure(CopyFiles(s3client, renames, opts), "cannot copy files");
}

}  // namespace iceberg::tools
