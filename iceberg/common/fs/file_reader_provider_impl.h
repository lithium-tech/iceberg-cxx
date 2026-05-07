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

  arrow::Result<std::shared_ptr<DeletionVector>> OpenDeletionVector(const std::string& path, int64_t offset,
                                                                    int64_t length) const override {
    ARROW_ASSIGN_OR_RAISE(auto input_file, opener_(path));
    ARROW_ASSIGN_OR_RAISE(auto footer, PuffinFile::ReadFooter(input_file));

    auto deserialized_footer = footer.GetDeserializedFooter();
    auto it =
        std::find_if(deserialized_footer.blobs.begin(), deserialized_footer.blobs.end(),
                     [offset, length](const auto& blob) { return blob.offset == offset && blob.length == length; });
    if (it == deserialized_footer.blobs.end()) {
      return arrow::Status::IOError("Deletion vector blob not found at offset ", offset);
    }
    ARROW_ASSIGN_OR_RAISE(auto buffer, input_file->ReadAt(offset, length));

    // TODO(MeT3ger): reorganize interface to avoid redundant copies of blob data
    std::string blob_data(reinterpret_cast<const char*>(buffer->data()), buffer->size());
    return std::make_shared<DeletionVector>(*it, std::move(blob_data));
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
