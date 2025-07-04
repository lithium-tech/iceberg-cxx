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

 public:
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
  struct ReaderGreater {
    bool operator()(std::shared_ptr<Reader> lhs, std::shared_ptr<Reader> rhs) const;
  };

  struct Query {
    std::string url;
    int64_t begin;
    int64_t end;
  };

  std::priority_queue<std::shared_ptr<Reader>, std::vector<std::shared_ptr<Reader>>, ReaderGreater> queue_;
  std::optional<Query> last_query_;

  std::shared_ptr<iceberg::ILogger> logger_;
};

}  // namespace iceberg
