#pragma once

#include <set>
#include <string>

#include "arrow/filesystem/filesystem.h"
#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "parquet/type_fwd.h"
#include "stats/data.h"
#include "stats/parquet/distinct.h"
#include "stats/parquet/frequent_item.h"
#include "stats/parquet/quantiles.h"

namespace stats {

enum class DistinctCounterImplType { kNaive, kTheta, kHyperLogLog };

// TODO(gmusya): implement SettingsBuilder
struct Settings {
  bool use_dictionary_optimization = false;
  DistinctCounterImplType distinct_counter_implementation = DistinctCounterImplType::kNaive;
  bool evaluate_quantiles = false;
  bool verbose = false;
  bool evaluate_frequent_items = false;
  std::set<std::string> columns_to_ignore;
  int32_t batch_size = 8192;

  // TODO(gmusya): remove filesystem from settings
  std::shared_ptr<arrow::fs::FileSystem> fs;
};

struct AnalyzeColumnResult {
  std::optional<stats::CommonDistinctWrapper> counter;
  std::optional<stats::CommonQuantileWrapper> quantile_sketch;
  std::optional<stats::CommonFrequentItemsWrapper> frequent_items_sketch;

  int32_t field_id;
  ParquetType type;
};

struct AnalyzeResult {
  std::map<std::string, AnalyzeColumnResult> sketches;
};

class Analyzer {
 public:
  Analyzer(Settings s) : settings_(std::move(s)) {}

  void Analyze(const std::string& filename);

  const AnalyzeResult& Result() { return result_; }

 private:
  AnalyzeColumnResult InitializeSketchesForColumn(const parquet::ColumnDescriptor& descriptor);

  bool EvaluateStatsFromDictionaryPage(const parquet::RowGroupMetaData& rg_metadata, int col,
                                       parquet::RowGroupReader& rg_reader);

  void AnalyzeColumn(const parquet::RowGroupMetaData& rg_metadata, int col, parquet::RowGroupReader& rg_reader);

  std::map<std::string, GenericParquetBuffer> buffers;
  AnalyzeResult result_;

  const Settings settings_;
};

}  // namespace stats
