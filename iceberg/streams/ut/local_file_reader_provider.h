#pragma once

#include <memory>
#include <string>

#include "arrow/filesystem/localfs.h"
#include "arrow/status.h"
#include "iceberg/common/fs/file_reader_provider.h"
#include "parquet/arrow/reader.h"

namespace iceberg {

class LocalFileReaderProvider : public IFileReaderProvider {
 public:
  arrow::Result<std::shared_ptr<parquet::arrow::FileReader>> Open(const std::string& url) const override {
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

    std::string path = url.starts_with("file://") ? url.substr(7) : url;
    ARROW_ASSIGN_OR_RAISE(auto input_file, fs->OpenInputFile(path));

    parquet::arrow::FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(reader_builder.Open(input_file));
    reader_builder.memory_pool(arrow::default_memory_pool());

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());

    return arrow_reader;
  }
};

}  // namespace iceberg
