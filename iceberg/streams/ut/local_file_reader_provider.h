#pragma once

#include <memory>
#include <string>

#include "arrow/filesystem/localfs.h"
#include "iceberg/common/fs/file_reader_provider_impl.h"

namespace iceberg {

inline std::shared_ptr<IFileReaderProvider> MakeLocalFileReaderProvider() {
  return std::make_shared<FileReaderProvider>(
      [](const std::string& url) -> arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> {
        auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
        std::string path = url.starts_with("file://") ? url.substr(7) : url;
        return fs->OpenInputFile(path);
      });
}

}  // namespace iceberg
