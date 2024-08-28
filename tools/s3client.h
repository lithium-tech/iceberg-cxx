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
      if (!arrow::fs::InitializeS3(global_options).ok()) {
        throw std::runtime_error("Cannot Initialize S3");
      }
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
  static constexpr int64_t CHUNK_SIZE = 1024 * 1024;

 public:
  S3Client(bool force, arrow::fs::S3LogLevel log_level = arrow::fs::S3LogLevel::Error,
           const std::string& src_env_prefix = "AWS_", const std::string& dst_env_prefix = "DST_")
      : s3init_(log_level) {
    src_access_.LoadEnvOptions(src_env_prefix);
    S3Access::ClearEnvOptions("AWS_");

    fs_ = std::make_shared<arrow::fs::LocalFileSystem>();

    auto s3fs_res = arrow::fs::S3FileSystem::Make(src_access_.options);
    if (!s3fs_res.ok()) {
      throw std::runtime_error(s3fs_res.status().ToString());
    }
    src_s3fs_ = *s3fs_res;

    if (!dst_env_prefix.empty()) {
      dst_access_.LoadEnvOptions(dst_env_prefix);
      s3fs_res = arrow::fs::S3FileSystem::Make(dst_access_.options);
      if (!s3fs_res.ok()) {
        throw std::runtime_error(s3fs_res.status().ToString());
      }
      dst_s3fs_ = *s3fs_res;
    }
  }

  arrow::fs::FileLocator MakeFileLocator(std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& name) const {
    arrow::fs::FileLocator file_loc{.filesystem = fs, .path = CropPrefix(name)};
    if (file_loc.path == name) {
      file_loc.filesystem = fs_;
    }
    if (!file_loc.filesystem) {
      throw std::runtime_error("fs is not set");
    }
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

  bool CopyFiles(const std::unordered_map<std::string, std::string>& renames, const CopyOptions& opts) {
    std::vector<arrow::fs::FileLocator> src;
    std::vector<arrow::fs::FileLocator> dst;
    src.reserve(renames.size());
    dst.reserve(renames.size());

    CheckFiles(renames, opts.no_check_dest, src, dst);
    if (src.empty()) {
      return true;
    }
    MakeDstDirs(dst);
    return CopyFiles(src, dst, opts);
  }

  bool CopyFiles(const std::vector<arrow::fs::FileLocator>& src, const std::vector<arrow::fs::FileLocator>& dst,
                 const CopyOptions& opts) {
    auto status = arrow::fs::CopyFiles(src, dst, arrow::io::default_io_context(), CHUNK_SIZE, opts.use_threads);
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
    if (!src_fs) {
      throw std::runtime_error("src fs is not set");
    }

    auto dst_fs = dst_s3fs_;
    auto dst_path = CropPrefix(dst);
    if (dst_path == dst) {
      dst_fs = fs_;
    }
    if (!dst_fs) {
      throw std::runtime_error("dst fs is not set");
    }

    auto status = arrow::fs::CopyFiles(src_fs, selector, dst_fs, dst_path, arrow::io::default_io_context(), CHUNK_SIZE,
                                       use_threads);
    if (!status.ok()) {
      throw std::runtime_error(status.ToString());
    }
    return true;
  }

 private:
  S3Init s3init_;
  S3Access src_access_;
  S3Access dst_access_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;
  std::shared_ptr<arrow::fs::FileSystem> src_s3fs_;
  std::shared_ptr<arrow::fs::FileSystem> dst_s3fs_;

  static std::vector<arrow::fs::FileInfo> GetFileInfos(const std::vector<arrow::fs::FileLocator>& locations) {
    std::vector<arrow::fs::FileInfo> out;
    out.reserve(locations.size());
    for (auto& [fs, path] : locations) {
      arrow::Result<arrow::fs::FileInfo> res = fs->GetFileInfo(path);
      out.emplace_back(res.ok() ? std::move(*res) : arrow::fs::FileInfo());
    }
    return out;
  }

  static bool CheckSame(const arrow::fs::FileInfo& src, const arrow::fs::FileInfo& dst) {
    // TODO(chertus): compare checksums
    return src.size() == dst.size();
  }

  void CheckFiles(const std::unordered_map<std::string, std::string>& renames, bool force_override,
                  std::vector<arrow::fs::FileLocator>& out_src, std::vector<arrow::fs::FileLocator>& out_dst) const {
    std::vector<arrow::fs::FileLocator> src_paths;
    src_paths.reserve(renames.size());
    std::vector<arrow::fs::FileLocator> dst_paths;
    dst_paths.reserve(renames.size());

    for (auto& [src_name, dst_name] : renames) {
      src_paths.push_back(SrcFileLocator(src_name));
      dst_paths.push_back(DstFileLocator(dst_name));
    }

    std::vector<arrow::fs::FileInfo> src_infos = GetFileInfos(src_paths);
    std::vector<arrow::fs::FileInfo> dst_infos;
    if (!force_override) {
      dst_infos = GetFileInfos(dst_paths);
    }

    for (size_t i = 0; i < src_infos.size(); ++i) {
      auto& src_info = src_infos[i];
      bool need_copy = src_info.IsFile();
      if (need_copy && !force_override) {
        need_copy = !CheckSame(src_info, dst_infos[i]);
      }
      if (need_copy) {
        out_src.emplace_back(std::move(src_paths[i]));
        out_dst.emplace_back(std::move(dst_paths[i]));
      }
    }
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

inline void CopyDirOrThrow(std::shared_ptr<S3Client> s3client, const std::string& src, const std::string& dst,
                           bool use_threads) {
  if (!CopyDir(s3client, src, dst, use_threads)) {
    throw std::runtime_error(std::string("cannot copy dir ") + src + " to " + dst);
  }
}

inline bool CopyFiles(std::shared_ptr<S3Client> s3client, const std::unordered_map<std::string, std::string>& renames,
                      const CopyOptions& opts) {
  if (s3client) {
    if (!s3client->CopyFiles(renames, opts)) {
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
  if (!CopyFiles(s3client, renames, opts)) {
    throw std::runtime_error("cannot copy files");
  }
}

}  // namespace iceberg::tools
