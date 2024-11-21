#pragma once

#include <cstdint>
#include <iostream>
#include <string>

#include "arrow/status.h"
#include "gen/src/compression.h"
#include "parquet/platform.h"

namespace gen {

struct WriteFlags {
  std::string output_dir;
  bool write_parquet = false;
  bool write_csv = false;
  parquet::Compression::type parquet_compression = parquet::Compression::UNCOMPRESSED;
  std::optional<int> compression_level = std::nullopt;

  friend std::ostream& operator<<(std::ostream& os, const WriteFlags& flags) {
    auto compression_str = ToString(flags.parquet_compression);
    os << "output_dir: " << flags.output_dir << ", write_parquet: " << flags.write_parquet
       << ", write_csv: " << flags.write_csv << ", parquet_compression: " << compression_str.value_or("unknown");
    if (flags.compression_level.has_value()) {
      os << ", compression_level: " << flags.compression_level.value();
    }
    return os;
  }
};

struct GenerateFlags {
  int32_t scale_factor = 1;
  int32_t arrow_batch_size = 8192;
  int64_t seed = 0;
  int32_t files_per_table = 1;
  bool use_equality_deletes = false;
  int32_t equality_deletes_columns_count = 0;
  double equality_deletes_rows_scale = 0;
  bool use_positional_deletes = false;
  double positional_deletes_rows_scale = 0;
  int32_t threads_to_use = 1;

  friend std::ostream& operator<<(std::ostream& os, const GenerateFlags& flags) {
    return os << "scale_factor: " << flags.scale_factor << ", arrow_batch_size: " << flags.arrow_batch_size
              << ", seed: " << flags.seed << ", files_per_table: " << flags.files_per_table
              << ", use_equality_deletes: " << flags.use_equality_deletes
              << ", equality_deletes_columns_count: " << flags.equality_deletes_columns_count
              << ", equality_deletes_rows_scale: " << flags.equality_deletes_rows_scale
              << ", use_positional_deletes: " << flags.use_positional_deletes
              << ", positional_deletes_rows_scale: " << flags.positional_deletes_rows_scale
              << ", threads_to_use: " << flags.threads_to_use;
  }
};

arrow::Status GenerateTPCH(const WriteFlags& write_flags, const GenerateFlags& generate_flags);

}  // namespace gen
