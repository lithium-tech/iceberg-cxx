#pragma once

#include <cstdint>

namespace iceberg {

struct PositionalDeleteStats {
  uint64_t files_read = 0;
  uint64_t rows_read = 0;

  void Combine(const PositionalDeleteStats& other) {
    files_read += other.files_read;
    rows_read += other.rows_read;
  }
};

}  // namespace iceberg