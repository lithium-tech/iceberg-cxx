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
#include "parquet/statistics.h"
#include "parquet/types.h"

namespace iceberg {

static bool Equals(parquet::ByteArray a, parquet::ByteArray b) {
  if (a.len != b.len) {
    return false;
  }
  if (a.ptr == b.ptr) {
    return true;
  }
  return std::memcmp(a.ptr, b.ptr, a.len) == 0;
};

class PositionalDeleteStream::Reader {
 public:
  explicit Reader(std::shared_ptr<parquet::arrow::FileReader> file_reader, Layer layer,
                  std::shared_ptr<iceberg::ILogger> logger)
      : file_reader_(std::move(file_reader)),
        parquet_reader_(file_reader_->parquet_reader()),
        layer_(layer),
        logger_(logger) {
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

  Layer layer() const noexcept { return layer_; }

  bool is_less(const Reader& other) const noexcept {
    return std::make_tuple(std::string_view(current_file_path_), current_pos_) <
           std::make_tuple(std::string_view(other.current_file_path_), other.current_pos_);
  }

  const parquet::RowGroupMetaData* current_row_group_metadata() const noexcept {
    return current_rowgroup_reader_->metadata();
  }

  /**
   * Advance to the next record.
   */
  bool Next() {
    while (true) {
      if (file_path_reader_ && file_path_reader_->HasNext()) {
        parquet::ByteArray file_path_value;
        int64_t pos;

        ReadValueNotNull(file_path_reader_, file_path_value);
        ReadValueNotNull(pos_reader_, pos);

        if (!Equals(current_file_path_value_, file_path_value)) {
          current_file_path_ = StringFromByteArray(file_path_value);
        }
        current_pos_ = pos;
        return true;
      }
      if (!MakeGroupReader()) {
        return false;
      }
    }
  }

  /**
   * Advance to the first record in the next row group.
   */
  bool NextRowGroup() {
    if (logger_ && current_row_group_ >= 0) {
      logger_->Log(std::to_string(rows_in_prev_row_groups_ - current_pos_), "metrics:positional:rows_skipped");
    }

    if (!MakeGroupReader()) {
      return false;
    }
    parquet::ByteArray file_path_value;
    int64_t pos;

    ReadValueNotNull(file_path_reader_, file_path_value);
    ReadValueNotNull(pos_reader_, pos);

    if (!Equals(current_file_path_value_, file_path_value)) {
      current_file_path_ = StringFromByteArray(file_path_value);
    }
    current_pos_ = pos;
    return true;
  }

 private:
  bool MakeGroupReader() {
    current_file_path_value_ = parquet::ByteArray();
    file_path_reader_.reset();
    pos_reader_.reset();

    ++current_row_group_;
    if (current_row_group_ >= num_row_groups_) {
      return false;
    }

    current_rowgroup_reader_ = parquet_reader_->RowGroup(current_row_group_);
    rows_in_prev_row_groups_ += current_rowgroup_reader_->metadata()->num_rows();
    auto column0 = current_rowgroup_reader_->Column(0);
    auto column1 = current_rowgroup_reader_->Column(1);
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

  // we must keep a reference to the reader to avoid the reader being destroyed
  std::shared_ptr<parquet::RowGroupReader> current_rowgroup_reader_;

  parquet::ByteArray current_file_path_value_;
  std::string current_file_path_;
  int64_t current_pos_{0};
  int num_row_groups_;
  int current_row_group_{-1};
  int64_t rows_in_prev_row_groups_{0};

  const Layer layer_;
  std::shared_ptr<iceberg::ILogger> logger_;
};

bool PositionalDeleteStream::BasicRowGroupFilter::Skip(const std::string& path,
                                                       const parquet::RowGroupMetaData* metadata, const Query& query) {
  auto column_metadata0 = metadata->ColumnChunk(0);
  auto column_metadata1 = metadata->ColumnChunk(1);
  if (!column_metadata0->is_stats_set() || !column_metadata1->is_stats_set()) {
    return false;
  }
  auto str_stats = static_pointer_cast<parquet::ByteArrayStatistics>(column_metadata0->statistics());
  auto int_stats = static_pointer_cast<parquet::Int64Statistics>(column_metadata1->statistics());

  bool only_one_url = (str_stats->HasDistinctCount() && str_stats->distinct_count() == 1);
  if (str_stats->HasMinMax()) {
    auto min_value = parquet::ByteArrayToString(str_stats->min());
    auto max_value = parquet::ByteArrayToString(str_stats->max());
    if (min_value > query.url || max_value < query.url) {
      return true;
    }
    only_one_url |= (str_stats->HasMinMax() && min_value == max_value);
  }

  if (!only_one_url || !int_stats->HasMinMax()) {
    return false;
  }
  return query.url != path || int_stats->max() < query.begin;
}

bool PositionalDeleteStream::ReaderGreater::operator()(std::shared_ptr<Reader> lhs, std::shared_ptr<Reader> rhs) const {
  return rhs->is_less(*lhs);
}

PositionalDeleteStream::PositionalDeleteStream(
    const std::map<Layer, std::vector<std::string>>& urls,
    const std::function<std::shared_ptr<parquet::arrow::FileReader>(const std::string&)>& cb,
    std::shared_ptr<iceberg::ILogger> logger, std::unique_ptr<RowGroupFilter> filter)
    : logger_(logger), filter_(std::move(filter)) {
  for (const auto& [layer, urls_for_layer] : urls) {
    for (const auto& url : urls_for_layer) {
      if (logger) {
        logger->Log(std::to_string(1), "metrics:positional:files_read");
      }
      auto reader = std::make_shared<Reader>(cb(url), layer, logger);

      if (reader->Next()) {
        queue_.push(reader);
      }
    }
  }
}

PositionalDeleteStream::PositionalDeleteStream(std::unique_ptr<parquet::arrow::FileReader> e, Layer layer,
                                               std::shared_ptr<iceberg::ILogger> logger,
                                               std::unique_ptr<RowGroupFilter> filter)
    : logger_(logger), filter_(std::move(filter)) {
  if (logger) {
    logger->Log(std::to_string(1), "metrics:positional:files_read");
  }
  auto reader = std::make_shared<Reader>(std::move(e), layer, logger);

  if (reader->Next()) {
    queue_.push(reader);
  }
}

// TODO(gmusya): get rid of this method
void PositionalDeleteStream::Append(UrlDeleteRows& rows) {
  while (!queue_.empty()) {
    const std::string url = queue_.top()->current_file_path();
    const auto& poses = GetDeleted(url, 0, INT64_MAX, 0);
    rows[url].insert(rows[url].end(), poses.begin(), poses.end());
  }

  for (auto& [_, poses] : rows) {
    std::sort(poses.begin(), poses.end());
    // Remove possible duplicates.
    poses.erase(std::unique(poses.begin(), poses.end()), poses.end());
  }
}

DeleteRows PositionalDeleteStream::GetDeleted(const std::string& url, int64_t begin, int64_t end, Layer data_layer) {
  if (last_query_.has_value()) {
    if (std::tie(last_query_->url, last_query_->end) > std::tie(url, begin)) {
      throw std::runtime_error("PositionalDeleteStream::GetDeleted: internal error in tea.");
    }
  }
  last_query_ = Query{.url = url, .begin = begin, .end = end};

  DeleteRows rows;
  int64_t rows_read = 0;

  while (!queue_.empty()) {
    const auto r = queue_.top();
    const auto cmp = url.compare(r->current_file_path());

    if (cmp < 0) {
      break;
    }
    if (cmp == 0 && r->current_pos() >= begin) {
      if (r->current_pos() < end) {
        if ((rows.empty() || rows.back() != r->current_pos()) && r->layer() >= data_layer) {
          rows.push_back(r->current_pos());
        }
      } else {
        break;
      }
    }
    queue_.pop();
    if (r->Next()) {
      queue_.push(r);
    }
    ++rows_read;
  }

  if (logger_) {
    logger_->Log(std::to_string(rows_read), "metrics:positional:rows_read");
  }
  return rows;
}

void PositionalDeleteStream::EnqueueOrDelete(bool cond, Reader* r) {
  if (cond) {
    queue_.push(r);
  } else if (readers_.erase(r)) {
    delete r;
  } else {
    assert(false);
  }
}

}  // namespace iceberg
