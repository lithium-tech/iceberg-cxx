#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <unordered_set>

#include "absl/flags/flag.h"
#include "absl/flags/internal/flag.h"
#include "absl/flags/parse.h"
#include "gen/src/writer.h"
#include "gen/tpch/dataset.h"

ABSL_FLAG(std::string, output_dir, "", "output directory");
ABSL_FLAG(int64_t, seed, 0, "seed for random device");
ABSL_FLAG(int32_t, arrow_batch_size, 8192, "arrow batch size (rows)");
ABSL_FLAG(int32_t, scale_factor, 1, "scale factor");
ABSL_FLAG(int32_t, files_per_table, 1, "files_per_table");
ABSL_FLAG(bool, write_parquet, false, "write parquet files");
ABSL_FLAG(bool, write_csv, false, "write csv files");
ABSL_FLAG(bool, use_equality_deletes, false, "either to generate equality delete files to all tables or not");
ABSL_FLAG(int32_t, equality_deletes_columns_count, 0, "number of columns in equality deletes files");
ABSL_FLAG(double, equality_deletes_rows_scale, 0, "row count ratio in equality delete files and in original table");
ABSL_FLAG(bool, use_positional_deletes, false, "either to generate positional delete files to all tables or not");
ABSL_FLAG(double, positional_deletes_rows_scale, 0, "row count ratio in equality delete files and in original table");
ABSL_FLAG(int32_t, threads_to_use, 1, "number of threads to use");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::string output_dir = absl::GetFlag(FLAGS_output_dir);
  int64_t seed = absl::GetFlag(FLAGS_seed);
  int32_t arrow_batch_size = absl::GetFlag(FLAGS_arrow_batch_size);
  int32_t scale_factor = absl::GetFlag(FLAGS_scale_factor);
  int32_t files_per_table = absl::GetFlag(FLAGS_files_per_table);
  bool write_parquet = absl::GetFlag(FLAGS_write_parquet);
  bool write_csv = absl::GetFlag(FLAGS_write_csv);

  if (write_csv) {
#ifndef HAS_ARROW_CSV
    std::cout << "arrow built without csv" << std::endl;
    return 1;
#endif
  }

  bool use_equality_deletes = absl::GetFlag(FLAGS_use_equality_deletes);
  int32_t equality_deletes_columns_count = absl::GetFlag(FLAGS_equality_deletes_columns_count);
  double equality_deletes_rows_scale = absl::GetFlag(FLAGS_equality_deletes_rows_scale);

  bool use_positional_deletes = absl::GetFlag(FLAGS_use_positional_deletes);
  double positional_deletes_rows_scale = absl::GetFlag(FLAGS_positional_deletes_rows_scale);

  int32_t threads_to_use = absl::GetFlag(FLAGS_threads_to_use);

  if (output_dir.empty()) {
    std::cerr << "output_dir must be set" << std::endl;
    return 1;
  }

  if (!write_parquet && !write_csv) {
    std::cerr << "Nothing to do. Please set --write_parquet or/and --write_csv" << std::endl;
    return 1;
  }

  gen::WriteFlags write_flags{.output_dir = output_dir, .write_parquet = write_parquet, .write_csv = write_csv};
  gen::GenerateFlags generate_flags{.scale_factor = scale_factor,
                                    .arrow_batch_size = arrow_batch_size,
                                    .seed = seed,
                                    .files_per_table = files_per_table,
                                    .use_equality_deletes = use_equality_deletes,
                                    .equality_deletes_columns_count = equality_deletes_columns_count,
                                    .equality_deletes_rows_scale = equality_deletes_rows_scale,
                                    .use_positional_deletes = use_positional_deletes,
                                    .positional_deletes_rows_scale = positional_deletes_rows_scale,
                                    .threads_to_use = threads_to_use};

  std::cerr << "write_flags: " << write_flags << std::endl;
  std::cerr << "generate_flags: " << generate_flags << std::endl;

  try {
    auto status = gen::GenerateTPCH(write_flags, generate_flags);

    if (!status.ok()) {
      std::cerr << status.message() << std::endl;
      return 1;
    }
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
  } catch (const arrow::Status& status) {
    std::cerr << status.message() << std::endl;
  }

  return 0;
}
