#pragma once

#include <cstdint>
#include <iostream>
#include <string>

#include "arrow/status.h"

namespace gen {

struct WriteFlags {
  std::string output_dir;
  bool write_parquet;
  bool write_csv;

  friend std::ostream& operator<<(std::ostream& os, const WriteFlags& flags) {
    return os << "output_dir: " << flags.output_dir << ", write_parquet: " << flags.write_parquet
              << ", write_csv: " << flags.write_csv;
  }
};

struct GenerateFlags {
  int32_t scale_factor;
  int32_t arrow_batch_size;
  int64_t seed;
  int32_t files_per_table;
  bool use_equality_deletes;
  int32_t equality_deletes_columns_count;
  double equality_deletes_rows_scale;
  bool use_positional_deletes;
  double positional_deletes_rows_scale;

  friend std::ostream& operator<<(std::ostream& os, const GenerateFlags& flags) {
    return os << "scale_factor: " << flags.scale_factor << ", arrow_batch_size: " << flags.arrow_batch_size
              << ", seed: " << flags.seed << ", files_per_table: " << flags.files_per_table
              << ", use_equality_deletes: " << flags.use_equality_deletes
              << ", equality_deletes_columns_count: " << flags.equality_deletes_columns_count
              << ", equality_deletes_rows_scale: " << flags.equality_deletes_rows_scale
              << ", use_positional_deletes: " << flags.use_positional_deletes
              << ", positional_deletes_rows_scale: " << flags.positional_deletes_rows_scale;
  }
};

arrow::Status GenerateTPCH(const WriteFlags& write_flags, const GenerateFlags& generate_flags);

}  // namespace gen
