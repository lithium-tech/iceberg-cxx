#pragma once

#include <algorithm>
#include <string>
#include <utility>

#include "iceberg/filter/representation/value.h"

namespace iceberg::filter {

template <ValueType value_type>
struct MinMaxStats {
  PhysicalType<value_type> min;
  PhysicalType<value_type> max;
};

inline bool operator==(const MinMaxStats<ValueType::kBool>& lhs, const MinMaxStats<ValueType::kBool>& rhs) {
  return std::tie(lhs.min, lhs.max) == std::tie(rhs.min, rhs.max);
}

static constexpr MinMaxStats<ValueType::kBool> kBoolStatsTrue{.min = true, .max = true};
static constexpr MinMaxStats<ValueType::kBool> kBoolStatsFalse{.min = false, .max = false};
static constexpr MinMaxStats<ValueType::kBool> kBoolStatsUnknown{.min = false, .max = true};

inline MinMaxStats<ValueType::kBool> operator||(MinMaxStats<ValueType::kBool> lhs, MinMaxStats<ValueType::kBool> rhs) {
  if (lhs == kBoolStatsTrue || rhs == kBoolStatsTrue) {
    return kBoolStatsTrue;
  }
  if (lhs == kBoolStatsFalse && rhs == kBoolStatsFalse) {
    return kBoolStatsFalse;
  }
  return kBoolStatsUnknown;
}

inline MinMaxStats<ValueType::kBool> operator&&(MinMaxStats<ValueType::kBool> lhs, MinMaxStats<ValueType::kBool> rhs) {
  if (lhs == kBoolStatsTrue && rhs == kBoolStatsTrue) {
    return kBoolStatsTrue;
  }
  if (lhs == kBoolStatsFalse || rhs == kBoolStatsFalse) {
    return kBoolStatsFalse;
  }
  return kBoolStatsUnknown;
}

inline MinMaxStats<ValueType::kBool> operator!(MinMaxStats<ValueType::kBool> arg) {
  return MinMaxStats<ValueType::kBool>{.min = !arg.max, .max = !arg.min};
}

struct CountStats {
  bool contains_null;
  bool contains_non_null;
};

template <ValueType value_type>
MinMaxStats<value_type> OneValueToStats(const PhysicalType<value_type>& value) {
  return MinMaxStats<value_type>{.min = value, .max = value};
}

class GenericMinMaxStats {
  using ValueHolder =
      std::variant<MinMaxStats<ValueType::kBool>, MinMaxStats<ValueType::kInt2>, MinMaxStats<ValueType::kInt4>,
                   MinMaxStats<ValueType::kInt8>, MinMaxStats<ValueType::kDate>, MinMaxStats<ValueType::kFloat4>,
                   MinMaxStats<ValueType::kFloat8>, MinMaxStats<ValueType::kInterval>, MinMaxStats<ValueType::kNumeric>,
                   MinMaxStats<ValueType::kString>, MinMaxStats<ValueType::kTime>, MinMaxStats<ValueType::kTimestamp>,
                   MinMaxStats<ValueType::kTimestamptz>>;

  template <typename Tag, ValueType value_type = Tag::value_type>
  GenericMinMaxStats(MinMaxStats<value_type> value, Tag) : value_type_(value_type), value_(std::move(value)) {}

 public:
  template <ValueType value_type>
  static GenericMinMaxStats Make(MinMaxStats<value_type> value) {
    return GenericMinMaxStats(std::move(value), Tag<value_type>{});
  }

  template <ValueType value_type>
  static GenericMinMaxStats Make(PhysicalType<value_type> min, PhysicalType<value_type> max) {
    MinMaxStats<value_type> stats{.min = std::move(min), .max = std::move(max)};
    return GenericMinMaxStats(std::move(stats), Tag<value_type>{});
  }

  template <ValueType value_type>
  static GenericMinMaxStats Make(const PhysicalType<value_type>& one_value) {
    MinMaxStats<value_type> stats{.min = one_value, .max = one_value};
    return GenericMinMaxStats(std::move(stats), Tag<value_type>{});
  }

  template <ValueType value_type>
  const MinMaxStats<value_type>& Get() const {
    return std::get<MinMaxStats<value_type>>(value_);
  }

  ValueType GetValueType() const { return value_type_; }

  const ValueHolder& GetHolder() const { return value_; }

 private:
  ValueType value_type_;

  ValueHolder value_;
};

struct GenericStats {
  GenericMinMaxStats min_max;
  CountStats count;

  GenericStats(GenericMinMaxStats min_max, CountStats count) : min_max(std::move(min_max)), count(std::move(count)) {}

  explicit GenericStats(GenericMinMaxStats min_max)
      : min_max(std::move(min_max)), count(CountStats{.contains_null = true, .contains_non_null = true}) {}
};

class IStatsGetter {
 public:
  virtual std::optional<GenericStats> GetStats(const std::string& column_name, filter::ValueType) const = 0;

  virtual ~IStatsGetter() = default;
};

}  // namespace iceberg::filter
