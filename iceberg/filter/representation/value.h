#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace iceberg::filter {

enum class ValueType {
  kBool,
  kInt2,
  kInt4,
  kInt8,
  kFloat4,
  kFloat8,
  kNumeric,
  kString,
  kDate,
  kTimestamp,
  kTimestamptz,
  kTime,
  kInterval,
};

std::string ValueTypeToString(ValueType);

template <ValueType value_type>
struct Traits {};

template <>
struct Traits<ValueType::kBool> {
  using PhysicalType = bool;
};

template <>
struct Traits<ValueType::kInt2> {
  using PhysicalType = int16_t;
};

template <>
struct Traits<ValueType::kInt4> {
  using PhysicalType = int32_t;
};

template <>
struct Traits<ValueType::kInt8> {
  using PhysicalType = int64_t;
};

template <>
struct Traits<ValueType::kFloat4> {
  using PhysicalType = float;
};

template <>
struct Traits<ValueType::kFloat8> {
  using PhysicalType = double;
};

struct Numeric {
  std::string value;

  bool operator==(const Numeric& other) const = default;
};

struct IntervalMonthMicro {
  int64_t months = 0;
  int64_t micros = 0;

  bool operator==(const IntervalMonthMicro& other) const = default;
};

template <>
struct Traits<ValueType::kNumeric> {
  using PhysicalType = Numeric;
};

template <>
struct Traits<ValueType::kString> {
  using PhysicalType = std::string;
};

template <>
struct Traits<ValueType::kDate> {
  using PhysicalType = int32_t;
};

template <>
struct Traits<ValueType::kTimestamp> {
  using PhysicalType = int64_t;
};

template <>
struct Traits<ValueType::kTimestamptz> {
  using PhysicalType = int64_t;
};

template <>
struct Traits<ValueType::kTime> {
  using PhysicalType = int64_t;
};

template <>
struct Traits<ValueType::kInterval> {
  using PhysicalType = IntervalMonthMicro;
};

class ArrayHolder;

template <ValueType value_type>
using PhysicalType = typename Traits<value_type>::PhysicalType;

template <ValueType value_type>
using PhysicalNullableType = std::optional<PhysicalType<value_type>>;

template <ValueType value_type>
using Array = std::vector<PhysicalNullableType<value_type>>;

template <ValueType valtype>
class Tag {
 public:
  static constexpr ValueType value_type = valtype;
};

using TagDispatcher =
    std::variant<Tag<ValueType::kBool>, Tag<ValueType::kInt2>, Tag<ValueType::kInt4>, Tag<ValueType::kInt8>,
                 Tag<ValueType::kDate>, Tag<ValueType::kFloat4>, Tag<ValueType::kFloat8>, Tag<ValueType::kInterval>,
                 Tag<ValueType::kNumeric>, Tag<ValueType::kString>, Tag<ValueType::kTime>, Tag<ValueType::kTimestamp>,
                 Tag<ValueType::kTimestamptz>>;

inline TagDispatcher DispatchTag(ValueType value_type) {
  switch (value_type) {
    case ValueType::kBool:
      return TagDispatcher(Tag<ValueType::kBool>());
    case ValueType::kInt2:
      return TagDispatcher(Tag<ValueType::kInt2>());
    case ValueType::kInt4:
      return TagDispatcher(Tag<ValueType::kInt4>());
    case ValueType::kInt8:
      return TagDispatcher(Tag<ValueType::kInt8>());
    case ValueType::kFloat4:
      return TagDispatcher(Tag<ValueType::kFloat4>());
    case ValueType::kFloat8:
      return TagDispatcher(Tag<ValueType::kFloat8>());
    case ValueType::kInterval:
      return TagDispatcher(Tag<ValueType::kInterval>());
    case ValueType::kNumeric:
      return TagDispatcher(Tag<ValueType::kNumeric>());
    case ValueType::kString:
      return TagDispatcher(Tag<ValueType::kString>());
    case ValueType::kTime:
      return TagDispatcher(Tag<ValueType::kTime>());
    case ValueType::kTimestamp:
      return TagDispatcher(Tag<ValueType::kTimestamp>());
    case ValueType::kTimestamptz:
      return TagDispatcher(Tag<ValueType::kTimestamptz>());
    case ValueType::kDate:
      return TagDispatcher(Tag<ValueType::kDate>());
  }
  throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": internal error"));
}

class ArrayHolder {
  using ValueHolder = std::variant<Array<ValueType::kBool>, Array<ValueType::kInt2>, Array<ValueType::kInt4>,
                                   Array<ValueType::kInt8>, Array<ValueType::kFloat4>, Array<ValueType::kFloat8>,
                                   Array<ValueType::kString>, Array<ValueType::kNumeric>, Array<ValueType::kInterval>>;

  template <typename Tag, ValueType value_type = Tag::value_type>
  ArrayHolder(Array<value_type> value, Tag) : value_type_(value_type), value_(std::move(value)) {}

 public:
  template <ValueType value_type>
  static ArrayHolder Make(Array<value_type> value) {
    return ArrayHolder(std::move(value), Tag<value_type>{});
  }

  template <ValueType value_type>
  const Array<value_type>& GetValue() const {
    return std::get<Array<value_type>>(value_);
  }

  size_t Size() const {
    return std::visit(
        [&]<ValueType value_type>(Tag<value_type>) -> size_t { return std::get<Array<value_type>>(value_).size(); },
        DispatchTag(value_type_));
  }

  ValueType GetValueType() const { return value_type_; }

  const ValueHolder& GetHolder() const { return value_; }

 private:
  ValueType value_type_;

  ValueHolder value_;
};

class Value {
  using ValueHolder = std::variant<PhysicalNullableType<ValueType::kBool>, PhysicalNullableType<ValueType::kInt2>,
                                   PhysicalNullableType<ValueType::kInt4>, PhysicalNullableType<ValueType::kInt8>,
                                   PhysicalNullableType<ValueType::kFloat4>, PhysicalNullableType<ValueType::kFloat8>,
                                   PhysicalNullableType<ValueType::kString>, PhysicalNullableType<ValueType::kNumeric>,
                                   PhysicalNullableType<ValueType::kInterval>>;

  template <typename Tag, ValueType value_type = Tag::value_type>
  Value(PhysicalNullableType<value_type> value, Tag) : value_type_(value_type), value_(std::move(value)) {}

 public:
  template <ValueType value_type>
  static Value Make() {
    return Value(PhysicalNullableType<value_type>{}, Tag<value_type>{});
  }

  template <ValueType value_type>
  static Value Make(PhysicalNullableType<value_type> value) {
    return Value(std::move(value), Tag<value_type>{});
  }

  template <ValueType value_type>
  const PhysicalType<value_type>& GetValue() const {
    const auto& maybe_value = GetOptional<value_type>();

    if (!maybe_value.has_value()) {
      throw std::runtime_error("Value (GetValue): value is not set");
    }

    return maybe_value.value();
  }

  template <ValueType value_type>
  const PhysicalNullableType<value_type>& GetOptional() const {
    if (value_type_ != value_type) {
      throw std::runtime_error("Value (GetOptional): expected value type " +
                               std::to_string(static_cast<int>(value_type)) + ", stored " +
                               std::to_string(static_cast<int>(value_type_)));
    }

    return std::get<PhysicalNullableType<value_type>>(value_);
  }

  bool IsNull() const {
    return std::visit([&](auto&& arg) { return !arg.has_value(); }, value_);
  }

  ValueType GetValueType() const { return value_type_; }

  const ValueHolder& GetHolder() const { return value_; }

 private:
  ValueType value_type_;

  ValueHolder value_;
};

}  // namespace iceberg::filter
