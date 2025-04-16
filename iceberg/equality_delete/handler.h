#pragma once

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "arrow/status.h"
#include "iceberg/equality_delete/common.h"
#include "iceberg/equality_delete/delete.h"
#include "iceberg/equality_delete/stats.h"
#include "iceberg/equality_delete/utils.h"

namespace iceberg {

// TODO(gmusya): make interface with different implementations
class EqualityDeleteHandler {
 public:
  struct Config {
    uint64_t max_rows;
    bool use_specialized_deletes;
    uint64_t equality_delete_max_mb_size;
    bool throw_if_memory_limit_exceeded;
  };

  explicit EqualityDeleteHandler(ReaderMethodType get_reader_method, const Config& config);

  arrow::Status AppendDelete(const std::string& url, const std::vector<FieldId>& field_ids);

  bool PrepareDeletesForFile();
  absl::flat_hash_set<FieldId> GetEqualityDeleteFieldIds() const;

  void PrepareDeletesForBatch(const std::map<FieldId, std::shared_ptr<arrow::Array>>& field_id_to_array);

  bool IsDeleted(uint64_t row) const;

  const EqualityDeleteStats& stats() const { return stats_; }

 private:
  EqualityDeleteStats stats_{};

  uint64_t current_rows_ = 0;
  const uint64_t max_rows_ = std::numeric_limits<uint64_t>::max();
  const bool use_specialized_deletes_ = true;

  ReaderMethodType open_url_method_;

  // after AppendDelete
  std::map<std::set<FieldId>, EqualityDeletePtr> materialized_deletes_;

  // after PrepareDeletesForFile
  std::vector<std::pair<const EqualityDelete*, std::set<FieldId>>> deletes_for_current_row_group_;

  // after PrepareDeleteForBatch
  using PreparedBatch = std::vector<std::shared_ptr<arrow::Array>>;
  std::vector<std::pair<const EqualityDelete*, PreparedBatch>> prepared_batches_;

  std::shared_ptr<MemoryState> shared_state_;
};

}  // namespace iceberg
