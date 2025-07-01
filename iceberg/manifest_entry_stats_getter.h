#pragma once

#include <memory>
#include <optional>
#include <string>

#include "iceberg/filter/stats_filter/stats.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/schema.h"

namespace iceberg {

// TODO(gmusya): mapping happens by lowercase name, not by id. Fix
class ManifestEntryStatsGetter : public iceberg::filter::IStatsGetter {
 public:
  ManifestEntryStatsGetter(const iceberg::ManifestEntry& entry, std::shared_ptr<const iceberg::Schema> schema)
      : entry_(entry), schema_(schema) {}

  std::optional<iceberg::filter::GenericStats> GetStats(const std::string& column_name,
                                                        iceberg::filter::ValueType value_type) const override;

 private:
  const iceberg::ManifestEntry& entry_;
  std::shared_ptr<const iceberg::Schema> schema_;
};

}  // namespace iceberg
