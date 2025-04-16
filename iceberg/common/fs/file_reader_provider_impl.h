#pragma once

#include <memory>
#include <string>

#include "arrow/status.h"
#include "iceberg/common/fs/file_reader_provider.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "parquet/arrow/reader.h"

namespace iceberg {

class FileReaderProvider : public IFileReaderProvider {
 public:
  explicit FileReaderProvider(std::shared_ptr<IFileSystemProvider> fs_provider) : fs_provider_(fs_provider) {}

  arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> Open(const std::string& url) const override {
    ARROW_ASSIGN_OR_RAISE(auto fs, fs_provider_->GetFileSystem(url));

    if (url.find("://") == std::string::npos) {
      return arrow::Status::ExecutionError("FileReaderProvider: file ", url, " does not contain schema");
    }

    std::string path = url.substr(url.find("://") + 3);
    ARROW_ASSIGN_OR_RAISE(auto input_file, fs->OpenInputFile(path));

    parquet::arrow::FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(reader_builder.Open(input_file));

    return reader_builder.Build();
  }

 private:
  std::shared_ptr<IFileSystemProvider> fs_provider_;
};

}  // namespace iceberg
