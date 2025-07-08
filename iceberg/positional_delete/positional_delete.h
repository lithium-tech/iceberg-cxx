#pragma once

#include <functional>

#include <iceberg/common/logger.h>

#include <map>
#include <memory>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "parquet/arrow/reader.h"

namespace iceberg {

using DeleteRows = std::vector<int64_t>;
using UrlDeleteRows = std::map<std::string, DeleteRows>;

/**
 * Merges data from multiple files with deletes into a unified stream.
 *
 * @note Format of position delete
 * https://iceberg.apache.org/spec/#delete-formats
 */
class PositionalDeleteStream {
  class Reader;
  class ReaderHolder {
   public:
    explicit ReaderHolder(std::shared_ptr<Reader> reader);

    std::shared_ptr<Reader> GetReader() const;

    // returns the minimal possible record that can be read from this reader
    std::pair<std::optional<std::string>, std::optional<int64_t>> GetPosition() const;

    bool IsStarted() const;

   private:
    std::shared_ptr<Reader> reader_;
  };

 public:
  struct Query {
    std::string url;
    int64_t begin;
    int64_t end;
  };
  class RowGroupFilter {
   public:
    enum class Result {
      kLess,
      kInter,
      kGreater,
    };

    // return kLess if all records in the RowGroup are less than the requested range
    // return kGreater if all records in the RowGroup (so in the reader as well) are greater than the requested range
    // return kInter otherwise
    // Note that if the statistics are not set and no assumptions can be made, the function will also return kInter
    static Result State(const parquet::RowGroupMetaData* metadata, const Query& query);
  };

  using Layer = int;

  PositionalDeleteStream(const std::map<Layer, std::vector<std::string>>& urls,
                         const std::function<std::shared_ptr<parquet::arrow::FileReader>(const std::string&)>& cb,
                         std::shared_ptr<iceberg::ILogger> logger = nullptr);

  /**
   * Get deleted rows in range [begin, end).
   *
   * @param url data file for which deleted rows are requeted
   * @param begin beginning of the requested range
   * @param end end of the requeted range
   */
  DeleteRows GetDeleted(const std::string& url, int64_t begin, int64_t end, Layer data_layer_number);

 private:
  struct ReaderHolderGreater {
    bool operator()(const ReaderHolder& lhs, const ReaderHolder& rhs) const;
  };

  void EnqueueIf(bool cond, ReaderHolder&& holder);

  std::priority_queue<ReaderHolder, std::vector<ReaderHolder>, ReaderHolderGreater> queue_;
  std::optional<Query> last_query_;

  std::shared_ptr<iceberg::ILogger> logger_;
};

}  // namespace iceberg
