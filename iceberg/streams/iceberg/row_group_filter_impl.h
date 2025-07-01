#pragma once

#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/stats_filter/stats_filter.h"
#include "iceberg/streams/arrow/error.h"
#include "iceberg/streams/iceberg/parquet_stats_getter.h"
#include "iceberg/streams/iceberg/row_group_filter.h"

namespace iceberg {

class RowGroupFilter : public iceberg::IRowGroupFilter {
 public:
  RowGroupFilter(iceberg::filter::NodePtr extracted_filter_representation,
                 std::unordered_map<int32_t, std::string> field_id_to_name, int64_t timestamp_to_timestamptz_shift_us)
      : extracted_filter_representation_(extracted_filter_representation),
        field_id_to_name_(std::move(field_id_to_name)),
        timestamp_to_timestamptz_shift_us_(timestamp_to_timestamptz_shift_us) {}

  bool CanSkipRowGroup(const parquet::RowGroupMetaData& meta) const override {
    iceberg::filter::StatsFilter filter(extracted_filter_representation_,
                                        iceberg::filter::StatsFilter::Settings{.timestamp_to_timestamptz_shift_us =
                                                                                   timestamp_to_timestamptz_shift_us_});
    return filter.ApplyFilter(GetCurrentParquetStatsGetter(meta)) == iceberg::filter::MatchedRows::kNone;
  }

 private:
  ParquetStatsGetter GetCurrentParquetStatsGetter(const parquet::RowGroupMetaData& meta) const {
    const parquet::SchemaDescriptor* schema = meta.schema();
    iceberg::Ensure(schema != nullptr, std::string(__PRETTY_FUNCTION__) + ": internal error. schema is nullptr");

    const parquet::schema::GroupNode* group_node = schema->group_node();
    iceberg::Ensure(group_node != nullptr,
                    std::string(__PRETTY_FUNCTION__) + ": internal error. group_node is nullptr");

    int field_count = group_node->field_count();

    std::unordered_map<std::string, int> name_to_parquet_index;

    for (int i = 0; i < field_count; ++i) {
      parquet::schema::NodePtr field_descr = group_node->field(i);
      iceberg::Ensure(field_descr != nullptr,
                      std::string(__PRETTY_FUNCTION__) + ": internal error. field_descr is nullptr");

      int32_t field_id = field_descr->field_id();
      if (field_id == -1) {
        continue;
      }

      if (field_id_to_name_.contains(field_id)) {
        name_to_parquet_index[field_id_to_name_.at(field_id)] = i;
      }
    }

    return ParquetStatsGetter(meta, name_to_parquet_index);
  }

  iceberg::filter::NodePtr extracted_filter_representation_;
  const std::unordered_map<int32_t, std::string> field_id_to_name_;
  const int64_t timestamp_to_timestamptz_shift_us_;
};

}  // namespace iceberg
