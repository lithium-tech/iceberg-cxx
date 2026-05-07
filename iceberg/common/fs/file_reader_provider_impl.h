#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>

#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/common/fs/file_reader_provider.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/puffin.h"
#include "iceberg/tea_scan.h"
#include "parquet/arrow/reader.h"

namespace iceberg {

class FileReaderProvider : public IFileReaderProvider {
 public:
  using RawOpener = std::function<arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>(const std::string&)>;

  explicit FileReaderProvider(RawOpener opener) : opener_(std::move(opener)) {}

  arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> OpenParquet(const std::string& url) const override {
    ARROW_ASSIGN_OR_RAISE(auto input_file, opener_(url));

    parquet::arrow::FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(reader_builder.Open(input_file));

    return reader_builder.Build();
  }

 private:
  RawOpener opener_;
};

inline std::shared_ptr<IFileReaderProvider> MakeFileReaderProvider(std::shared_ptr<IFileSystemProvider> fs_provider) {
  return std::make_shared<FileReaderProvider>([fs_provider = std::move(fs_provider)](const std::string& url)
                                                  -> arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> {
    ARROW_ASSIGN_OR_RAISE(auto fs, fs_provider->GetFileSystem(url));

    if (url.find("://") == std::string::npos) {
      return arrow::Status::ExecutionError("FileReaderProvider: file ", url, " does not contain schema");
    }

    std::string path = url.substr(url.find("://") + 3);
    return fs->OpenInputFile(path);
  });
}

}  // namespace iceberg
