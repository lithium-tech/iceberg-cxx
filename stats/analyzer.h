#pragma once

#include <set>

#include <stats/datasketch/distinct.h>

#include <string>

#include "arrow/filesystem/filesystem.h"
#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "parquet/type_fwd.h"
#include "stats/data.h"
#include "stats/datasketch/frequent_items.h"
#include "stats/datasketch/quantiles.h"
#include "stats/measure.h"

namespace stats {

enum class DistinctCounterImplType { kNaive, kTheta, kHyperLogLog };

// TODO(gmusya): implement SettingsBuilder
struct Settings {
  bool use_dictionary_optimization = false;
  bool use_precalculation_optimization = false;
  bool use_string_view_heuristic = false;
  std::optional<int> row_groups_limit = std::nullopt;
  DistinctCounterImplType distinct_counter_implementation = DistinctCounterImplType::kTheta;
  bool evaluate_distinct = false;
  bool evaluate_quantiles = false;
  bool verbose = false;
  bool print_timings = false;
  bool evaluate_frequent_items = false;
  std::set<std::string> columns_to_ignore;
  std::set<std::string> columns_to_process;
  int32_t batch_size = 8192;
  bool read_all_data = false;

  // TODO(gmusya): remove filesystem from settings
  std::shared_ptr<arrow::fs::FileSystem> fs;
};

struct AnalyzeColumnResult {
  std::optional<stats::GenericDistinctCounterSketch> counter;
  std::optional<stats::GenericQuantileSketch> quantile_sketch;
  std::optional<stats::GenericFrequentItemsSketch> frequent_items_sketch;

  std::optional<stats::GenericQuantileSketch> quantile_sketch_dictionary;
  std::optional<stats::GenericFrequentItemsSketch> frequent_items_sketch_dictionary;

  int32_t field_id;
  ParquetType type;
};

struct AnalyzeResult {
  std::map<std::string, AnalyzeColumnResult> sketches;
};

struct Metrics {
  iceberg::DurationClock reading_{};

  iceberg::DurationClock distinct_{};
  iceberg::DurationClock quantile_{};
  iceberg::DurationClock frequent_items_{};
  iceberg::DurationClock counting_{};
};

class Analyzer {
 public:
  Analyzer(Settings s) : settings_(std::move(s)) {}

  void Analyze(const std::string& filename);

  const AnalyzeResult& Result() { return result_; }

 private:
  bool EvaluateStatsFromDictionaryPage(const parquet::RowGroupMetaData& rg_metadata, int col,
                                       parquet::RowGroupReader& rg_reader);

  void AnalyzeColumn(const parquet::RowGroupMetaData& rg_metadata, int col, parquet::RowGroupReader& rg_reader);

  std::map<std::string, GenericParquetBuffer> buffers;
  std::map<std::string, GenericParquetDictionaryBuffer> dictionary_buffers;
  std::map<std::string, std::vector<int32_t>> count_buffers;
  AnalyzeResult result_;

  mutable std::map<std::string, Metrics> metrics_per_column_;

  const Settings settings_;
};

}  // namespace stats
