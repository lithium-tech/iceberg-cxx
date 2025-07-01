#include "iceberg/streams/iceberg/parquet_stats_getter.h"

#include <algorithm>
#include <string>
#include <utility>

#include "iceberg/filter/representation/value.h"
#include "parquet/metadata.h"
#include "parquet/statistics.h"
#include "parquet/types.h"

namespace iceberg {

namespace {
using iceberg::filter::ValueType;
using LogicalTypePtr = std::shared_ptr<const parquet::LogicalType>;

template <ValueType value_type>
bool ValdateLogicalType(LogicalTypePtr) {
  return false;
}

template <>
bool ValdateLogicalType<ValueType::kBool>(LogicalTypePtr type) {
  return type->is_none();
}

template <>
bool ValdateLogicalType<ValueType::kInt2>(LogicalTypePtr type) {
  if (type->is_none()) {
    return true;
  }
  if (type->is_int()) {
    auto int_type = std::static_pointer_cast<const parquet::IntLogicalType>(type);
    return int_type->is_signed() && int_type->bit_width() == 16;
  }
  return false;
}

template <>
bool ValdateLogicalType<ValueType::kInt4>(LogicalTypePtr type) {
  if (type->is_none()) {
    return true;
  }
  if (type->is_int()) {
    auto int_type = std::static_pointer_cast<const parquet::IntLogicalType>(type);
    return int_type->is_signed() && (int_type->bit_width() == 16 || int_type->bit_width() == 32);
  }
  return false;
}

template <>
bool ValdateLogicalType<ValueType::kInt8>(LogicalTypePtr type) {
  if (type->is_none()) {
    return true;
  }
  if (type->is_int()) {
    auto int_type = std::static_pointer_cast<const parquet::IntLogicalType>(type);
    return int_type->is_signed() && int_type->bit_width() == 64;
  }
  return false;
}

template <>
bool ValdateLogicalType<ValueType::kString>(LogicalTypePtr type) {
  return type->is_string();
}

template <>
bool ValdateLogicalType<ValueType::kFloat4>(LogicalTypePtr type) {
  return type->is_none();
}

template <>
bool ValdateLogicalType<ValueType::kFloat8>(LogicalTypePtr type) {
  return type->is_none();
}

template <>
bool ValdateLogicalType<ValueType::kDate>(LogicalTypePtr type) {
  return type->is_date();
}

template <>
bool ValdateLogicalType<ValueType::kTime>(LogicalTypePtr type) {
  if (type->is_time()) {
    auto time_type = std::static_pointer_cast<const parquet::TimeLogicalType>(type);
    return !time_type->is_adjusted_to_utc() && time_type->time_unit() == parquet::LogicalType::TimeUnit::MICROS;
  }
  return false;
}

template <>
bool ValdateLogicalType<ValueType::kTimestamp>(LogicalTypePtr type) {
  if (type->is_timestamp()) {
    auto timestamp_type = std::static_pointer_cast<const parquet::TimestampLogicalType>(type);
    return !timestamp_type->is_adjusted_to_utc() &&
           timestamp_type->time_unit() == parquet::LogicalType::TimeUnit::MICROS;
  }
  return false;
}

template <>
bool ValdateLogicalType<ValueType::kTimestamptz>(LogicalTypePtr type) {
  if (type->is_timestamp()) {
    auto timestamp_type = std::static_pointer_cast<const parquet::TimestampLogicalType>(type);
    return timestamp_type->is_adjusted_to_utc() &&
           timestamp_type->time_unit() == parquet::LogicalType::TimeUnit::MICROS;
  }
  return false;
}

// TODO(gmusya): support numeric
// template <>
// bool ValdateLogicalType<ValueType::kNumeric>(LogicalTypePtr type) {
//   return type->is_decimal();
// }

}  // namespace

template <parquet::Type::type parquet_type, iceberg::filter::ValueType value_type>
std::optional<iceberg::filter::GenericStats> GenericStatsFromTypedStats(std::shared_ptr<parquet::Statistics> stats,
                                                                        LogicalTypePtr logical_type) {
  if (!ValdateLogicalType<value_type>(logical_type)) {
    return std::nullopt;
  }

  iceberg::filter::CountStats count_stats;
  if (!stats->HasNullCount()) {
    count_stats.contains_non_null = true;
    count_stats.contains_null = true;
  } else {
    count_stats.contains_non_null = stats->num_values() > 0;
    count_stats.contains_null = stats->null_count() > 0;
  }

  using PhysicalType = parquet::PhysicalType<parquet_type>;
  using TypedStatistics = parquet::TypedStatistics<PhysicalType>;

  auto typed_stats = std::static_pointer_cast<TypedStatistics>(stats);
  if constexpr (parquet_type == parquet::Type::BYTE_ARRAY) {
    parquet::ByteArray min_value = typed_stats->min();
    parquet::ByteArray max_value = typed_stats->max();
    std::string min(reinterpret_cast<const char*>(min_value.ptr), min_value.len);
    std::string max(reinterpret_cast<const char*>(max_value.ptr), max_value.len);
    auto min_max_stats = iceberg::filter::GenericMinMaxStats::Make<value_type>(std::move(min), std::move(max));
    return iceberg::filter::GenericStats(std::move(min_max_stats), std::move(count_stats));
  } else {
    auto min_max_stats = iceberg::filter::GenericMinMaxStats::Make<value_type>(typed_stats->min(), typed_stats->max());
    return iceberg::filter::GenericStats(std::move(min_max_stats), std::move(count_stats));
  }
}

ParquetStatsGetter::ParquetStatsGetter(const parquet::RowGroupMetaData& rg_meta,
                                       std::unordered_map<std::string, int> name_to_parquet_index)
    : rg_meta_(rg_meta), name_to_parquet_index_(std::move(name_to_parquet_index)) {}

using StatsConverter = std::add_pointer<std::optional<iceberg::filter::GenericStats>(
    std::shared_ptr<parquet::Statistics>, LogicalTypePtr)>::type;

struct AnnotatedStatsConverter {
  parquet::Type::type parquet_physical_type;
  iceberg::filter::ValueType filter_value_type;
  StatsConverter stats_converter;
};

std::optional<iceberg::filter::GenericStats> ParquetStatsGetter::GetStats(const std::string& column_name,
                                                                          iceberg::filter::ValueType value_type) const {
  auto it = name_to_parquet_index_.find(column_name);
  if (it == name_to_parquet_index_.end()) {
    return std::nullopt;
  }
  auto parquet_index = it->second;
  if (parquet_index < 0 || parquet_index >= rg_meta_.num_columns()) {
    return std::nullopt;
  }
  auto col_meta = rg_meta_.ColumnChunk(parquet_index);
  if (!col_meta->is_stats_set() || !col_meta->statistics()) {
    return std::nullopt;
  }

  std::shared_ptr<parquet::Statistics> stats = col_meta->statistics();
  if (!stats->HasMinMax()) {
    return std::nullopt;
  }

  auto physical_type = stats->physical_type();
  auto logical_type = rg_meta_.schema()->Column(parquet_index)->logical_type();
  using ParType = parquet::Type;
  using ValType = iceberg::filter::ValueType;

#define CONVERSION(PAR_TYPE, VAL_TYPE) \
  { PAR_TYPE, VAL_TYPE, GenericStatsFromTypedStats<PAR_TYPE, VAL_TYPE> }

  constexpr AnnotatedStatsConverter kConversions[] = {
      CONVERSION(ParType::BOOLEAN, ValType::kBool),      CONVERSION(ParType::INT32, ValType::kInt2),
      CONVERSION(ParType::INT32, ValType::kInt4),        CONVERSION(ParType::INT64, ValType::kInt8),
      CONVERSION(ParType::FLOAT, ValType::kFloat4),      CONVERSION(ParType::DOUBLE, ValType::kFloat8),
      CONVERSION(ParType::BYTE_ARRAY, ValType::kString), CONVERSION(ParType::INT64, ValType::kTime),
      CONVERSION(ParType::INT64, ValType::kTimestamp),   CONVERSION(ParType::INT64, ValType::kTimestamptz),
      CONVERSION(ParType::INT32, ValType::kDate)};

#undef CONVERSION

  for (const auto& possible_conversion : kConversions) {
    if (possible_conversion.parquet_physical_type == physical_type &&
        possible_conversion.filter_value_type == value_type) {
      return possible_conversion.stats_converter(stats, logical_type);
    }
  }
  return std::nullopt;
}

}  // namespace iceberg
