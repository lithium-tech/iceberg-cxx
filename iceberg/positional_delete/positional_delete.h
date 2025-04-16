#pragma once

#include <functional>
#include <map>
#include <memory>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "iceberg/positional_delete/stats.h"
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
  PositionalDeleteStream(const std::set<std::string>& urls,
                         const std::function<std::shared_ptr<parquet::arrow::FileReader>(const std::string&)>& cb);

  explicit PositionalDeleteStream(std::unique_ptr<parquet::arrow::FileReader> file);

  ~PositionalDeleteStream();

  void Append(UrlDeleteRows& rows);

  /**
   * Get deleted rows in range [begin, end).
   *
   * @param url data file for which deleted rows are requeted
   * @param begin beginning of the requested range
   * @param end end of the requeted range
   */
  DeleteRows GetDeleted(const std::string& url, int64_t begin, int64_t end);

  const PositionalDeleteStats stats() const { return stats_; }

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
  PositionalDeleteStats stats_ = {};
  std::optional<Query> last_query_;
};

}  // namespace iceberg
