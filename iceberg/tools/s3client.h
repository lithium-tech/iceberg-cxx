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

namespace ice_tea {

struct S3Access {
  static constexpr const char* ENDPOINT_URL = "ENDPOINT_URL";
  static constexpr const char* DEFAULT_REGION = "DEFAULT_REGION";
  static constexpr const char* ACCESS_KEY_ID = "ACCESS_KEY_ID";
  static constexpr const char* SECRET_ACCESS_KEY = "SECRET_ACCESS_KEY";
  static constexpr const char* AWS_EC2_METADATA_DISABLED = "AWS_EC2_METADATA_DISABLED";

  arrow::fs::S3Options options;

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
      if (!arrow::fs::InitializeS3(global_options).ok()) {
        throw std::runtime_error("Cannot Initialize S3");
      }
    }
  }

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
  static constexpr int64_t CHUNK_SIZE = 1024 * 1024;

 public:
  S3Client(bool force, arrow::fs::S3LogLevel log_level = arrow::fs::S3LogLevel::Error,
           const std::string& src_env_prefix = "AWS_", const std::string& dst_env_prefix = "DST_")
      : continue_on_fail_(force) {
    S3Access::InitS3(log_level);

    src_access_.LoadEnvOptions(src_env_prefix);
    dst_access_.LoadEnvOptions(dst_env_prefix);
    S3Access::ClearEnvOptions("AWS_");

    fs_ = std::make_shared<arrow::fs::LocalFileSystem>();

    auto s3fs_res = arrow::fs::S3FileSystem::Make(src_access_.options);
    if (!s3fs_res.ok()) {
      throw std::runtime_error(s3fs_res.status().ToString());
    }
    src_s3fs_ = *s3fs_res;

    s3fs_res = arrow::fs::S3FileSystem::Make(dst_access_.options);
    if (!s3fs_res.ok()) {
      throw std::runtime_error(s3fs_res.status().ToString());
    }
    dst_s3fs_ = *s3fs_res;
  }

  ~S3Client() {
    if (!arrow::fs::IsS3Finalized()) {
      arrow::fs::FinalizeS3().ok();
    }
  }

  bool CopyFiles(const std::unordered_map<std::string, std::string>& renames, bool use_threads) {
    std::unordered_set<std::string> found = CheckFiles(renames);

    std::vector<arrow::fs::FileLocator> src;
    std::vector<arrow::fs::FileLocator> dst;
    src.reserve(renames.size());
    dst.reserve(renames.size());

    for (auto& [src_name, dst_name] : renames) {
      arrow::fs::FileLocator from{.filesystem = src_s3fs_, .path = CropPrefix(src_name)};
      if (from.path == src_name) {
        from.filesystem = fs_;
      }

      if (!found.contains(from.path)) {
        if (!continue_on_fail_) {
          throw std::runtime_error(std::string("file not found ") + src_name);
        }
        continue;
      }

      arrow::fs::FileLocator to{.filesystem = dst_s3fs_, .path = CropPrefix(dst_name)};
      if (to.path == dst_name) {
        to.filesystem = fs_;
      }

      src.emplace_back(std::move(from));
      dst.emplace_back(std::move(to));
    }

    auto status = arrow::fs::CopyFiles(src, dst, arrow::io::default_io_context(), CHUNK_SIZE, use_threads);
    if (!status.ok()) {
      throw std::runtime_error(status.ToString());
    }
    return true;
  }

  bool CopyDir(const std::string& src, const std::string& dst, bool use_threads) {
    arrow::fs::FileSelector selector;
    selector.allow_not_found = false;
    selector.recursive = true;
    selector.base_dir = CropPrefix(src);

    auto src_fs = src_s3fs_;
    if (selector.base_dir == src) {
      src_fs = fs_;
    }

    auto dst_fs = dst_s3fs_;
    auto dst_path = CropPrefix(dst);
    if (dst_path == dst) {
      dst_fs = fs_;
    }

    auto status = arrow::fs::CopyFiles(src_fs, selector, dst_fs, dst_path, arrow::io::default_io_context(), CHUNK_SIZE,
                                       use_threads);
    if (!status.ok()) {
      throw std::runtime_error(status.ToString());
    }
    return true;
  }

 private:
  bool continue_on_fail_;
  S3Access src_access_;
  S3Access dst_access_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;
  std::shared_ptr<arrow::fs::FileSystem> src_s3fs_;
  std::shared_ptr<arrow::fs::FileSystem> dst_s3fs_;

  static arrow::fs::FileInfoVector GetFileInfos(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                const std::vector<std::string>& paths) {
    if (paths.empty()) {
      return {};
    }

    arrow::Result<arrow::fs::FileInfoVector> res = fs->GetFileInfo(paths);
    if (!res.ok()) {
      throw std::runtime_error(res.status().ToString());
    }
    return *res;
  }

  std::unordered_set<std::string> CheckFiles(const std::unordered_map<std::string, std::string>& renames) const {
    std::unordered_set<std::string> found;
    std::vector<std::string> src_s3_paths;
    src_s3_paths.reserve(renames.size());

    std::vector<std::string> src_local_paths;
    src_local_paths.reserve(renames.size());

    for (auto& [src_name, _] : renames) {
      auto name = CropPrefix(src_name);
      if (name == src_name) {
        src_local_paths.push_back(name);
      } else {
        src_s3_paths.push_back(name);
      }
    }

    for (auto& info : GetFileInfos(src_s3fs_, src_s3_paths)) {
      if (info.IsFile()) {
        found.insert(info.path());
      }
    }
    for (auto& info : GetFileInfos(fs_, src_local_paths)) {
      if (info.IsFile()) {
        found.insert(info.path());
      }
    }
    return found;
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

inline bool CopyDir(std::shared_ptr<S3Client> s3client, const std::string& src, const std::string& dst,
                    bool use_threads) {
  if (s3client) {
    if (!s3client->CopyDir(src, dst, use_threads)) {
      return false;
    }
  } else if (!Rclone("copy", src, dst)) {
    return false;
  }
  return true;
}

inline bool CopyFiles(std::shared_ptr<S3Client> s3client, const std::unordered_map<std::string, std::string>& renames,
                      bool use_threads) {
  if (s3client) {
    if (!s3client->CopyFiles(renames, use_threads)) {
      return false;
    }
  } else {
    for (auto& [src, dst] : renames) {
      if (!Rclone("copyto", src, dst)) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace ice_tea
