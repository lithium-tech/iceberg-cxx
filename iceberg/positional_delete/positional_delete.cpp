#include "iceberg/positional_delete/positional_delete.h"

#include <algorithm>
#include <cassert>
#include <set>
#include <string>
#include <utility>

#include "arrow/status.h"
#include "iceberg/common/logger.h"
#include "parquet/arrow/reader.h"
#include "parquet/column_reader.h"
#include "parquet/types.h"

// #include "tea/observability/positional_delete_stats.h"

namespace iceberg {

class PositionalDeleteStream::Reader {
 public:
  explicit Reader(std::shared_ptr<parquet::arrow::FileReader> file_reader, std::shared_ptr<iceberg::ILogger> logger)
      : file_reader_(std::move(file_reader)), parquet_reader_(file_reader_->parquet_reader()), logger_(logger) {
    auto file_metadata = parquet_reader_->metadata();
    auto num_columns = file_metadata->num_columns();

    // [spec] Position-based delete files store file_position_delete, a struct
    // with the following fields:
    // * file_path - Full URI of a data file with FS scheme.
    // * pos - Ordinal position of a deleted row in the target data file.
    // * row - Deleted row values. Omit the column when not storing deleted
    // rows.
    if (!(num_columns == 2 || num_columns == 3)) {
      throw arrow::Status::ExecutionError("Unexpected number of columns in delete files: ", num_columns);
    }

    num_row_groups_ = file_metadata->num_row_groups();
  }

  const std::string& current_file_path() const noexcept { return current_file_path_; }

  int64_t current_pos() const noexcept { return current_pos_; }

  bool is_less(const Reader& other) const noexcept {
    return std::make_tuple(std::string_view(current_file_path_), current_pos_) <
           std::make_tuple(std::string_view(other.current_file_path_), other.current_pos_);
  }

  /**
   * Advance to the next record.
   */
  bool Next() {
    const auto compare_byte_array = [](parquet::ByteArray a, parquet::ByteArray b) {
      if (a.len != b.len) {
        return false;
      }
      if (a.ptr == b.ptr) {
        return true;
      }
      return std::memcmp(a.ptr, b.ptr, a.len) == 0;
    };

    int64_t rows_read = 0;

    while (true) {
      if (file_path_reader_ && file_path_reader_->HasNext()) {
        parquet::ByteArray file_path_value;
        int64_t pos;

        ++rows_read;
        ReadValueNotNull(file_path_reader_, file_path_value);
        ReadValueNotNull(pos_reader_, pos);

        if (!compare_byte_array(current_file_path_value_, file_path_value)) {
          current_file_path_ = StringFromByteArray(file_path_value);
        }
        current_pos_ = pos;
        if (logger_) {
          logger_->Log(std::to_string(rows_read), "metrics:positional:rows_read");
        }
        return true;
      }
      if (!MakeGroupReader()) {
        if (logger_) {
          logger_->Log(std::to_string(rows_read), "metrics:positional:rows_read");
        }
        return false;
      }
    }
  }

 private:
  bool MakeGroupReader() {
    current_file_path_value_ = parquet::ByteArray();
    file_path_reader_.reset();
    pos_reader_.reset();

    if (next_row_group_ >= num_row_groups_) {
      return false;
    }

    auto row_group_reader = parquet_reader_->RowGroup(next_row_group_++);
    auto column0 = row_group_reader->Column(0);
    auto column1 = row_group_reader->Column(1);
    // Validate type of the columns.
    if (column0->type() != parquet::Type::BYTE_ARRAY) {
      throw arrow::Status::ExecutionError("Unexpected column 0 type ", column0->type());
    }
    if (column1->type() != parquet::Type::INT64) {
      throw arrow::Status::ExecutionError("Unexpected column 1 type ", column1->type());
    }

    file_path_reader_ = std::static_pointer_cast<parquet::ByteArrayReader>(column0);
    pos_reader_ = std::static_pointer_cast<parquet::Int64Reader>(column1);

    return true;
  }

  template <typename Reader, typename Value>
  void ReadValueNotNull(Reader& reader, Value& value) {
    int64_t values_read;
    int64_t rows_read = reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
    if (rows_read != 1 || values_read != 1) {
      throw arrow::Status::ExecutionError("Delete file: unexpected number of rows read ", rows_read, " values read ",
                                          values_read);
    }
  }

  std::string_view StringFromByteArray(const parquet::ByteArray& value) const noexcept {
    return {reinterpret_cast<const char*>(value.ptr), value.len};
  }

 private:
  std::shared_ptr<parquet::arrow::FileReader> file_reader_;
  parquet::ParquetFileReader* parquet_reader_;
  std::shared_ptr<parquet::ByteArrayReader> file_path_reader_;
  std::shared_ptr<parquet::Int64Reader> pos_reader_;

  parquet::ByteArray current_file_path_value_;
  std::string current_file_path_;
  int64_t current_pos_;
  int num_row_groups_;
  int next_row_group_{0};
  std::shared_ptr<iceberg::ILogger> logger_;
};

bool PositionalDeleteStream::ReaderGreater::operator()(const Reader* lhs, const Reader* rhs) const {
  return rhs->is_less(*lhs);
}

PositionalDeleteStream::PositionalDeleteStream(
    const std::set<std::string>& urls,
    const std::function<std::shared_ptr<parquet::arrow::FileReader>(const std::string&)>& cb,
    std::shared_ptr<iceberg::ILogger> logger)
    : logger_(logger) {
  readers_.reserve(urls.size());

  for (const auto& url : urls) {
    if (logger) {
      logger->Log(std::to_string(1), "metrics:positional:files_read");
    }
    auto reader = std::make_unique<Reader>(cb(url), logger);

    if (reader->Next()) {
      queue_.push(reader.get());
      readers_.insert(reader.release());
    }
  }
}

PositionalDeleteStream::PositionalDeleteStream(std::unique_ptr<parquet::arrow::FileReader> e,
                                               std::shared_ptr<iceberg::ILogger> logger)
    : logger_(logger) {
  if (logger) {
    logger->Log(std::to_string(1), "metrics:positional:files_read");
  }
  auto reader = std::make_unique<Reader>(std::move(e), logger);

  if (reader->Next()) {
    queue_.push(reader.get());
    readers_.insert(reader.release());
  }
}

PositionalDeleteStream::~PositionalDeleteStream() {
  for (auto r : readers_) {
    delete r;
  }
}

void PositionalDeleteStream::Append(UrlDeleteRows& rows) {
  while (!queue_.empty()) {
    const std::string url = queue_.top()->current_file_path();
    const auto& poses = GetDeleted(url, 0, INT64_MAX);
    rows[url].insert(rows[url].end(), poses.begin(), poses.end());
  }

  for (auto& [_, poses] : rows) {
    std::sort(poses.begin(), poses.end());
    // Remove possible duplicates.
    poses.erase(std::unique(poses.begin(), poses.end()), poses.end());
  }
}

DeleteRows PositionalDeleteStream::GetDeleted(const std::string& url, int64_t begin, int64_t end) {
  if (last_query_.has_value()) {
    if (std::tie(last_query_->url, last_query_->end) > std::tie(url, begin)) {
      throw std::runtime_error("PositionalDeleteStream::GetDeleted: internal error in tea.");
    }
  }
  last_query_ = Query{.url = url, .begin = begin, .end = end};

  DeleteRows rows;

  while (!queue_.empty()) {
    const auto r = queue_.top();
    const auto cmp = url.compare(r->current_file_path());

    if (cmp < 0) {
      break;
    }
    if (cmp == 0 && r->current_pos() >= begin) {
      if (r->current_pos() < end) {
        if (rows.empty() || rows.back() != r->current_pos()) {
          rows.push_back(r->current_pos());
        }
      } else {
        break;
      }
    }
    queue_.pop();
    EnqueueOrDelete(r);
  }

  return rows;
}

void PositionalDeleteStream::EnqueueOrDelete(Reader* r) {
  if (r->Next()) {
    queue_.push(r);
  } else if (readers_.erase(r)) {
    delete r;
  } else {
    assert(false);
  }
}

}  // namespace iceberg
