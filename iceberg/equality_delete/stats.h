#pragma once

#include <algorithm>
#include <cstdint>

namespace iceberg {

struct EqualityDeleteStats {
  uint64_t files_read = 0;
  uint64_t rows_read = 0;
  uint64_t max_rows_materialized = 0;
  double max_mb_size_materialized = 0;

  void Combine(const EqualityDeleteStats& other) {
    files_read += other.files_read;
    rows_read += other.rows_read;
    max_rows_materialized = std::max(max_rows_materialized, other.max_rows_materialized);
    max_mb_size_materialized = std::max(max_mb_size_materialized, other.max_mb_size_materialized);
  }
};

}  // namespace iceberg
