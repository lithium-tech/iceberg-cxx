#pragma once

#include <functional>

#include <iceberg/common/logger.h>

#include <map>
#include <memory>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
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

  // used only for testing purposes. DO NOT USE IN PRODUCTION CODE
  // TODO(gmusya): remove this constructor
  explicit PositionalDeleteStream(std::unique_ptr<parquet::arrow::FileReader> file, Layer delete_layer,
                                  std::shared_ptr<iceberg::ILogger> logger = nullptr);

  ~PositionalDeleteStream();

  // used only for testing purposes. DO NOT USE IN PRODUCTION CODE
  // TODO(gmusya): remove this method
  void Append(UrlDeleteRows& rows);

  /**
   * Get deleted rows in range [begin, end).
   *
   * @param url data file for which deleted rows are requeted
   * @param begin beginning of the requested range
   * @param end end of the requeted range
   */
  DeleteRows GetDeleted(const std::string& url, int64_t begin, int64_t end, Layer data_layer_number);

 private:
  void EnqueueOrDelete(Reader*);

 private:
  struct ReaderGreater {
    bool operator()(const Reader* lhs, const Reader* rhs) const;
  };

  struct Query {
    std::string url;
    int64_t begin;
    int64_t end;
  };

  absl::flat_hash_set<Reader*> readers_;
  std::priority_queue<Reader*, std::vector<Reader*>, ReaderGreater> queue_;
  std::optional<Query> last_query_;

  std::shared_ptr<iceberg::ILogger> logger_;
};

}  // namespace iceberg
