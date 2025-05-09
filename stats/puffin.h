#pragma once

#include "iceberg/puffin.h"
#include "iceberg/table_metadata.h"
#include "stats/analyzer.h"

namespace stats {

void SketchesToPuffin(const stats::AnalyzeResult& result, iceberg::PuffinFileBuilder& puffin_file_builder);

iceberg::Statistics PuffinInfoToStatistics(const iceberg::PuffinFile& puffin_file, const std::string& puffin_file_path,
                                           int64_t snapshot_id);

}  // namespace stats
