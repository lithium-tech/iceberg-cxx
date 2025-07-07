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

    if (file_metadata->schema()->Column(0)->physical_type() != parquet::Type::BYTE_ARRAY) {
      throw arrow::Status::ExecutionError("Unexpected column 0 type " +
                                          parquet::TypeToString(file_metadata->schema()->Column(0)->physical_type()));
    }

    if (file_metadata->schema()->Column(1)->physical_type() != parquet::Type::INT64) {
      throw arrow::Status::ExecutionError("Unexpected column 1 type ",
                                          parquet::TypeToString(file_metadata->schema()->Column(1)->physical_type()));
    }

    num_row_groups_ = file_metadata->num_row_groups();
  }

  const std::optional<std::string>& current_file_path() const noexcept { return current_file_path_; }

  std::optional<int64_t> current_pos() const noexcept { return current_pos_; }

  Layer layer() const noexcept { return layer_; }

  const parquet::RowGroupMetaData* current_row_group_metadata() const noexcept {
    return current_rowgroup_reader_->metadata();
  }

  /**
   * Advance to the next record.
   */
  bool Next() {
    if (!file_path_reader_) {
      // already checked column types in the constructor
      file_path_reader_ = std::static_pointer_cast<parquet::ByteArrayReader>(current_rowgroup_reader_->Column(0));
      pos_reader_ = std::static_pointer_cast<parquet::Int64Reader>(current_rowgroup_reader_->Column(1));
    }
    if (file_path_reader_->HasNext()) {
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
    return NextRowGroup();
  }

  bool NextRowGroup() {
    current_file_path_value_ = parquet::ByteArray();
    file_path_reader_.reset();
    pos_reader_.reset();

    ++current_row_group_;
    if (current_row_group_ >= num_row_groups_) {
      return false;
    }

    current_rowgroup_reader_ = parquet_reader_->RowGroup(current_row_group_);
    current_pos_.reset();
    current_file_path_.reset();

    return true;
  }

 private:
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
  std::optional<std::string> current_file_path_;
  std::optional<int64_t> current_pos_;
  int num_row_groups_;
  int current_row_group_{-1};

  const Layer layer_;
  std::shared_ptr<iceberg::ILogger> logger_;
};

std::shared_ptr<PositionalDeleteStream::Reader> PositionalDeleteStream::ReaderHolder::GetReader() const {
  return reader_;
}

PositionalDeleteStream::ReaderHolder::ReaderHolder(std::shared_ptr<PositionalDeleteStream::Reader> reader)
    : reader_(reader) {}

std::pair<std::optional<std::string>, std::optional<int64_t>> PositionalDeleteStream::ReaderHolder::GetPosition()
    const {
  std::pair<std::optional<std::string>, std::optional<int64_t>> res(reader_->current_file_path(),
                                                                    reader_->current_pos());
  if (IsStarted()) {
    return res;
  }

  auto column_metadata0 = reader_->current_row_group_metadata()->ColumnChunk(0);
  auto column_metadata1 = reader_->current_row_group_metadata()->ColumnChunk(1);
  if (!column_metadata0->is_stats_set() || !column_metadata1->is_stats_set()) {
    return res;
  }
  // already checked column types in the reader's constructor
  auto str_stats = static_pointer_cast<parquet::ByteArrayStatistics>(column_metadata0->statistics());
  auto int_stats = static_pointer_cast<parquet::Int64Statistics>(column_metadata1->statistics());
  if (!str_stats->HasMinMax()) {
    return res;
  }
  res.first = parquet::ByteArrayToString(str_stats->min());

  if (int_stats->HasMinMax()) {
    res.second = int_stats->min();
  }
  return res;
}

bool PositionalDeleteStream::ReaderHolderGreater::operator()(const ReaderHolder& lhs, const ReaderHolder& rhs) const {
  return lhs.GetPosition() > rhs.GetPosition();
}

bool PositionalDeleteStream::ReaderHolder::IsStarted() const { return reader_->current_pos().has_value(); }

PositionalDeleteStream::BasicRowGroupFilter::Result PositionalDeleteStream::BasicRowGroupFilter::State(
    const parquet::RowGroupMetaData* metadata, const Query& query) {
  auto column_metadata0 = metadata->ColumnChunk(0);
  auto column_metadata1 = metadata->ColumnChunk(1);
  if (!column_metadata0->is_stats_set() || !column_metadata1->is_stats_set()) {
    return Result::kInter;
  }
  // already checked column types in the reader's constructor
  auto str_stats = static_pointer_cast<parquet::ByteArrayStatistics>(column_metadata0->statistics());
  auto int_stats = static_pointer_cast<parquet::Int64Statistics>(column_metadata1->statistics());
  if (!str_stats->HasMinMax()) {
    return Result::kInter;
  }

  auto min_value = parquet::ByteArrayToString(str_stats->min());
  auto max_value = parquet::ByteArrayToString(str_stats->max());
  if (max_value < query.url) {
    return Result::kLess;
  }
  if (min_value > query.url) {
    return Result::kGreater;
  }
  if (min_value != max_value || !int_stats->HasMinMax()) {
    return Result::kInter;
  }
  if (int_stats->max() < query.begin) {
    return Result::kLess;
  }
  if (int_stats->min() >= query.end) {
    return Result::kGreater;
  }
  return Result::kInter;
}

PositionalDeleteStream::PositionalDeleteStream(
    const std::map<Layer, std::vector<std::string>>& urls,
    const std::function<std::shared_ptr<parquet::arrow::FileReader>(const std::string&)>& cb,
    std::shared_ptr<RowGroupFilter> filter, std::shared_ptr<iceberg::ILogger> logger)
    : filter_(filter), logger_(logger) {
  for (const auto& [layer, urls_for_layer] : urls) {
    for (const auto& url : urls_for_layer) {
      if (logger) {
        logger->Log(std::to_string(1), "metrics:positional:files_read");
      }
      auto reader = std::make_shared<Reader>(cb(url), layer, logger);

      if (reader->NextRowGroup()) {
        queue_.push(ReaderHolder(reader));
      }
    }
  }
}

void PositionalDeleteStream::EnqueueIf(bool cond, ReaderHolder&& holder) {
  if (cond) {
    queue_.push(std::move(holder));
  }
}

DeleteRows PositionalDeleteStream::GetDeleted(const std::string& url, int64_t begin, int64_t end, Layer data_layer) {
  if (last_query_.has_value()) {
    if (std::tie(last_query_->url, last_query_->end) > std::tie(url, begin)) {
      throw std::runtime_error("Internal error in " + std::string(__PRETTY_FUNCTION__) +
                               ": queries must be disjoint and in ascending order");
    }
  }
  last_query_ = Query{.url = url, .begin = begin, .end = end};

  DeleteRows rows;
  int64_t rows_read = 0;

  while (!queue_.empty()) {
    auto holder = queue_.top();
    auto r = holder.GetReader();
    queue_.pop();

    // Row Group will be skipped if there is no filter, or filter returns kInter, meaning
    // query intersects with the row group data or we have too little information to decide

    if (!holder.IsStarted()) {
      if (filter_) {
        auto res = filter_->State(r->current_row_group_metadata(), last_query_.value());
        if (res == RowGroupFilter::Result::kLess) {
          if (logger_) {
            logger_->Log(std::to_string(r->current_row_group_metadata()->num_rows()),
                         "metrics:positional:rows_skipped");
          }
          EnqueueIf(r->NextRowGroup(), std::move(holder));
          continue;
        }
        if (res == RowGroupFilter::Result::kGreater) {
          queue_.push(holder);
          break;
        }
      }
      EnqueueIf(r->Next(), std::move(holder));
      continue;
    }

    const auto cmp = url.compare(*r->current_file_path());

    if (cmp < 0) {
      queue_.push(holder);
      break;
    }
    if (cmp == 0 && r->current_pos() >= begin) {
      if (r->current_pos() < end) {
        if ((rows.empty() || rows.back() != r->current_pos()) && r->layer() >= data_layer) {
          rows.push_back(*r->current_pos());
        }
      } else {
        queue_.push(holder);
        break;
      }
    }
    EnqueueIf(r->Next(), std::move(holder));
    ++rows_read;
  }

  if (logger_) {
    logger_->Log(std::to_string(rows_read), "metrics:positional:rows_read");
  }
  return rows;
}

}  // namespace iceberg
