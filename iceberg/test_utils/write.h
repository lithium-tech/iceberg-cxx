#pragma once

#include <string>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/common.h"
#include "iceberg/test_utils/scoped_temp_dir.h"

namespace iceberg {

arrow::Status WriteToFile(const Table& table, const std::string& file_path);

arrow::Status WriteToFile(const std::vector<ParquetColumn>& columns, const std::string& file_path);

arrow::Status WriteToFile(const ParquetColumn& column, const std::string& file_path);

class IFileWriter {
 public:
  struct Hints {
    std::optional<std::vector<size_t>> row_group_sizes;
    std::optional<std::string> desired_file_suffix;
  };

  virtual arrow::Result<FilePath> WriteFile(const std::vector<ParquetColumn>& columns, const Hints& hints) = 0;

  virtual ~IFileWriter() = default;
};

class LocalFileWriter : public IFileWriter {
 public:
  LocalFileWriter() {}

  arrow::Result<FilePath> WriteFile(const std::vector<ParquetColumn>& columns, const Hints& hints) override {
    std::vector<size_t> rg_sizes{hints.row_group_sizes.value_or(std::vector<size_t>{columns[0].Size()})};
    std::string file_suffix = hints.desired_file_suffix.value_or("file" + std::to_string(files_written_) + ".parquet");
    ++files_written_;
    FilePath file_path = "file://" + data_dir_.path().string() + "/" + file_suffix;

    ARROW_RETURN_NOT_OK(
        WriteToFile(Table{.columns = std::move(columns), .row_group_sizes = std::move(rg_sizes)}, file_path));
    return file_path;
  }

 private:
  ScopedTempDir data_dir_;
  int32_t files_written_ = 0;
};

}  // namespace iceberg
