#pragma once

#include <memory>
#include <string>

#include "arrow/result.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/stats_filter/stats.h"

namespace iceberg::filter {

enum class MatchedRows { kNone = 0, kSome = 1, kAll = 2 };

class StatsFilter {
 public:
  struct Settings {
    std::optional<int64_t> timestamp_to_timestamptz_shift_us = std::nullopt;
  };

  explicit StatsFilter(iceberg::filter::NodePtr root, Settings settings);

  MatchedRows ApplyFilter(const IStatsGetter& stats_getter) const;

  class ILogger {
   public:
    virtual void Log(const std::string& message) = 0;
    virtual ~ILogger() = default;
  };

  void SetLogger(std::shared_ptr<ILogger> logger) { logger_ = logger; }

  arrow::Result<GenericStats> Evaluate(const IStatsGetter& stats_getter) const;

 private:
  iceberg::filter::NodePtr root_;
  std::shared_ptr<ILogger> logger_;

  Settings settings_;
};

}  // namespace iceberg::filter
