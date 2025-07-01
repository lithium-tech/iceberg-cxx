#include "iceberg/manifest_entry_stats_getter.h"

#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/ascii.h"
#include "iceberg/filter/representation/value.h"
#include "iceberg/filter/stats_filter/stats.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {
using iceberg::filter::ValueType;
using IceId = iceberg::TypeID;

template <ValueType value_type>
using PhysType = iceberg::filter::PhysicalType<value_type>;

template <IceId type_id, ValueType value_type>
std::optional<PhysType<value_type>> DeserializeValue(const std::vector<uint8_t>& value) {
  return std::nullopt;
}

template <ValueType value_type>
std::optional<PhysType<value_type>> DeserializeWithMemcpy(const std::vector<uint8_t>& value) {
  if (value.size() != sizeof(PhysType<value_type>)) {
    return std::nullopt;
  }
  PhysType<value_type> result;
  std::memcpy(&result, value.data(), sizeof(result));
  return result;
}

template <>
std::optional<PhysType<ValueType::kBool>> DeserializeValue<IceId::kBoolean, ValueType::kBool>(
    const std::vector<uint8_t>& value) {
  return DeserializeWithMemcpy<ValueType::kBool>(value);
}

template <>
std::optional<PhysType<ValueType::kInt4>> DeserializeValue<IceId::kInt, ValueType::kInt4>(
    const std::vector<uint8_t>& value) {
  return DeserializeWithMemcpy<ValueType::kInt4>(value);
}

template <>
std::optional<PhysType<ValueType::kInt8>> DeserializeValue<IceId::kLong, ValueType::kInt8>(
    const std::vector<uint8_t>& value) {
  return DeserializeWithMemcpy<ValueType::kInt8>(value);
}

#if 0
template <>
std::optional<PhysType<ValueType::kFloat4>> DeserializeValue<IceId::kFloat, ValueType::kFloat4>(
    const std::vector<uint8_t>& value) {
  return DeserializeWithMemcpy<ValueType::kFloat4>(value);
}

template <>
std::optional<PhysType<ValueType::kFloat8>> DeserializeValue<IceId::kDouble, ValueType::kFloat8>(
    const std::vector<uint8_t>& value) {
  return DeserializeWithMemcpy<ValueType::kFloat8>(value);
}
#endif

template <>
std::optional<PhysType<ValueType::kDate>> DeserializeValue<IceId::kDate, ValueType::kDate>(
    const std::vector<uint8_t>& value) {
  return DeserializeWithMemcpy<ValueType::kDate>(value);
}

template <>
std::optional<PhysType<ValueType::kTime>> DeserializeValue<IceId::kTime, ValueType::kTime>(
    const std::vector<uint8_t>& value) {
  return DeserializeWithMemcpy<ValueType::kTime>(value);
}

template <>
std::optional<PhysType<ValueType::kTimestamp>> DeserializeValue<IceId::kTimestamp, ValueType::kTimestamp>(
    const std::vector<uint8_t>& value) {
  return DeserializeWithMemcpy<ValueType::kTimestamp>(value);
}

template <>
std::optional<PhysType<ValueType::kInt2>> DeserializeValue<IceId::kInt, ValueType::kInt2>(
    const std::vector<uint8_t>& value) {
  auto maybe_int4 = DeserializeWithMemcpy<ValueType::kInt4>(value);
  if (!maybe_int4.has_value()) {
    return std::nullopt;
  }
  int32_t int4 = maybe_int4.value();
  if (!(std::numeric_limits<int16_t>::min() <= int4 && int4 <= std::numeric_limits<int16_t>::max())) {
    return std::nullopt;
  }
  return PhysType<ValueType::kInt2>(int4);
}

template <>
std::optional<PhysType<ValueType::kString>> DeserializeValue<IceId::kString, ValueType::kString>(
    const std::vector<uint8_t>& value) {
  std::string result;
  result.resize(value.size());
  std::memcpy(result.data(), value.data(), value.size());
  return PhysType<ValueType::kString>(std::move(result));
}

template <>
std::optional<PhysType<ValueType::kTimestamptz>> DeserializeValue<IceId::kTimestamptz, ValueType::kTimestamptz>(
    const std::vector<uint8_t>& value) {
  return DeserializeWithMemcpy<ValueType::kTimestamptz>(value);
}

template <iceberg::TypeID type_id, ValueType value_type>
std::optional<iceberg::filter::GenericMinMaxStats> GenericStatsFromTypedStats(const std::vector<uint8_t>& min,
                                                                              const std::vector<uint8_t>& max) {
  auto min_value = DeserializeValue<type_id, value_type>(min);
  auto max_value = DeserializeValue<type_id, value_type>(max);
  if (!min_value.has_value() || !max_value.has_value()) {
    return std::nullopt;
  }
  return iceberg::filter::GenericMinMaxStats::Make<value_type>(std::move(min_value.value()),
                                                               std::move(max_value.value()));
}

using StatsConverter = std::optional<iceberg::filter::GenericMinMaxStats> (*)(const std::vector<uint8_t>& min,
                                                                              const std::vector<uint8_t>& max);

StatsConverter TypesToStatsConverter(iceberg::TypeID ice_type, iceberg::filter::ValueType value_type) {
  using ValType = iceberg::filter::ValueType;
  using iceberg::TypeID;

#define CONVERSION(ICE_TYPE, VAL_TYPE) \
  TypesToConverter { ICE_TYPE, VAL_TYPE, GenericStatsFromTypedStats<ICE_TYPE, VAL_TYPE> }

  constexpr struct TypesToConverter {
    iceberg::TypeID iceberg_type_id;
    iceberg::filter::ValueType filter_value_type;
    StatsConverter stats_converter;
  } kConversions[] = {CONVERSION(TypeID::kBoolean, ValType::kBool),
                      CONVERSION(TypeID::kInt, ValType::kInt2),
                      CONVERSION(TypeID::kInt, ValType::kInt4),
                      CONVERSION(TypeID::kLong, ValType::kInt8),
                      CONVERSION(TypeID::kFloat, ValType::kFloat4),
                      CONVERSION(TypeID::kDouble, ValType::kFloat8),
                      CONVERSION(TypeID::kString, ValType::kString),
                      CONVERSION(TypeID::kTime, ValType::kTime),
                      CONVERSION(TypeID::kTimestamp, ValType::kTimestamp),
                      CONVERSION(TypeID::kTimestamptz, ValType::kTimestamptz),
                      CONVERSION(TypeID::kDate, ValType::kDate)};

#undef CONVERSION

  for (const auto& possible_conversion : kConversions) {
    if (possible_conversion.iceberg_type_id == ice_type && possible_conversion.filter_value_type == value_type) {
      return possible_conversion.stats_converter;
    }
  }

  return nullptr;
}
}  // namespace

std::optional<iceberg::filter::GenericStats> ManifestEntryStatsGetter::GetStats(
    const std::string& column_name, iceberg::filter::ValueType value_type) const {
  const auto& columns = schema_->Columns();
  std::optional<iceberg::types::NestedField> maybe_field;
  for (const auto& col : columns) {
    if (absl::AsciiStrToLower(col.name) == absl::AsciiStrToLower(column_name)) {
      maybe_field = col;
      break;
    }
  }
  if (!maybe_field.has_value()) {
    return std::nullopt;
  }
  const auto field = std::move(maybe_field.value());

  // Extract column field_id from mapped column
  const int field_id = field.field_id;

  const auto& upper_bounds = entry_.data_file.upper_bounds;
  const auto& lower_bounds = entry_.data_file.lower_bounds;

  if (!upper_bounds.contains(field_id) || !lower_bounds.contains(field_id)) {
    return std::nullopt;
  }

  // Use field_id to extract bounds
  const auto& max_bytes = upper_bounds.at(field_id);
  const auto& min_bytes = lower_bounds.at(field_id);

  auto maybe_conversion = TypesToStatsConverter(field.type->TypeId(), value_type);
  if (!maybe_conversion) {
    return std::nullopt;
  }
  auto maybe_min_max_stats = maybe_conversion(min_bytes, max_bytes);
  if (maybe_min_max_stats.has_value()) {
    return iceberg::filter::GenericStats(std::move(maybe_min_max_stats.value()));
  }
  return std::nullopt;
}

}  // namespace iceberg
