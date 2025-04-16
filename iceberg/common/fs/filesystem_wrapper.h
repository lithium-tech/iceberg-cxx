#pragma once

#include <memory>

#include "arrow/filesystem/filesystem.h"
#include "arrow/io/file.h"
#include "arrow/result.h"

namespace iceberg {

// Usage example:
// * Create InputFileImpl : InputFileWrapper
// * Create FileSystemImpl : FileSystemWrapper
// * Override OpenInputFile method of FileSystemImpl to return InputFileImpl
// * Override some methods of InputFileWrapper to invoke callbacks before calling InputFileWrapper methods

class InputFileWrapper : public arrow::io::RandomAccessFile {
 public:
  explicit InputFileWrapper(std::shared_ptr<arrow::io::RandomAccessFile> file) : file_(file) {}

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    return file_->ReadAt(position, nbytes, out);
  }

  arrow::Status Close() override { return file_->Close(); }

  bool closed() const override { return file_->closed(); }

  arrow::Result<int64_t> Tell() const override { return file_->Tell(); }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override { return file_->Read(nbytes, out); }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override { return file_->Read(nbytes); }

  arrow::Status Seek(int64_t position) override { return file_->Seek(position); }

  arrow::Result<int64_t> GetSize() override { return file_->GetSize(); }

 private:
  std::shared_ptr<arrow::io::RandomAccessFile> file_;
};

class FileSystemWrapper : public arrow::fs::SubTreeFileSystem {
 public:
  explicit FileSystemWrapper(std::shared_ptr<arrow::fs::FileSystem> fs) : arrow::fs::SubTreeFileSystem("", fs) {}
};

}  // namespace iceberg
