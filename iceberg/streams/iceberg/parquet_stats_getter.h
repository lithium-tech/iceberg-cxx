#pragma once

#include <optional>
#include <string>
#include <unordered_map>

#include "iceberg/filter/stats_filter/stats.h"
#include "parquet/metadata.h"

namespace iceberg {

class ParquetStatsGetter : public iceberg::filter::IStatsGetter {
 public:
  ParquetStatsGetter(const parquet::RowGroupMetaData& rg_meta,
                     std::unordered_map<std::string, int> name_to_parquet_index);

  std::optional<iceberg::filter::GenericStats> GetStats(const std::string& column_name,
                                                        iceberg::filter::ValueType value_type) const override;

 private:
  const parquet::RowGroupMetaData& rg_meta_;
  std::unordered_map<std::string, int> name_to_parquet_index_;
};

}  // namespace iceberg
