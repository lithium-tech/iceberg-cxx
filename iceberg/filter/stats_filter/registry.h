#pragma once

#include <vector>

#include "arrow/result.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/stats_filter/stats.h"

namespace iceberg::filter {

using StatsValue = arrow::Result<GenericStats>;

class Registry {
 public:
  struct Settings {
    std::optional<int64_t> timestamp_to_timestamptz_shift_us = std::nullopt;
  };

  explicit Registry(Settings settings) : settings_(settings) {}

  arrow::Result<GenericStats> Evaluate(FunctionID function_id, std::vector<GenericStats> args) const;

 private:
  arrow::Result<std::vector<GenericStats>> CastToSameTypeForComparison(std::vector<GenericStats> args) const;

  Settings settings_;
};

}  // namespace iceberg::filter
