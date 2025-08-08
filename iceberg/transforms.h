#pragma once

#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include <chrono>
#include <limits>
#include <memory>
#include <sstream>
#include <type_traits>

#include "iceberg/hash_utils.h"
#include "iceberg/type.h"

namespace iceberg {

struct PartitionKey {
  std::vector<std::shared_ptr<arrow::Scalar>> partition_values_;

  bool operator==(const PartitionKey& other) const;

  std::string ToPath() const;
};

struct PartitionKeyHasher {
  uint64_t operator()(const PartitionKey& key) const;
};

static constexpr const char* identity_transform_prefix = "identity";
static constexpr const char* void_transform_prefix = "void";
static constexpr const char* bucket_transform_prefix = "bucket";
static constexpr const char* truncate_transform_prefix = "truncate";
static constexpr const char* year_transform_prefix = "year";
static constexpr const char* day_transform_prefix = "day";
static constexpr const char* month_transform_prefix = "month";
static constexpr const char* hour_transform_prefix = "hour";

enum class TransformType {
  kIdentityTransform,
  kVoidTransform,
  kBucketTransform,
  kTruncateTransform,
  kYearTransform,
  kDayTransform,
  kMonthTransform,
  kHourTransform
};

std::string GetTransformNameByType(TransformType type);

struct ITransform {
  virtual bool CanTransform(const std::shared_ptr<types::Type>& src) const = 0;

  virtual std::string ToString() const = 0;

  virtual std::shared_ptr<types::Type> ResultType(const std::shared_ptr<types::Type>& src) const = 0;

  virtual std::shared_ptr<arrow::Scalar> Transform(const std::shared_ptr<arrow::Scalar>& src) = 0;

  virtual ~ITransform() = default;
};

class IdentityTransform : public ITransform {
 public:
  std::shared_ptr<arrow::Scalar> Transform(const std::shared_ptr<arrow::Scalar>& src) override { return src; }

  bool CanTransform(const std::shared_ptr<types::Type>& src) const override { return true; }

  std::string ToString() const override { return identity_transform_prefix; }

  std::shared_ptr<types::Type> ResultType(const std::shared_ptr<types::Type>& src) const override { return src; }
};

class BucketTransform : public ITransform {
 public:
  explicit BucketTransform(int num_buckets) : num_buckets_(num_buckets) {}

  std::shared_ptr<arrow::Scalar> Transform(const std::shared_ptr<arrow::Scalar>& src) override {
    return std::make_shared<arrow::Int32Scalar>(
        (MurMurScalarHashImpl(*src).hash_ & std::numeric_limits<int32_t>::max()) % num_buckets_);
  }

  bool CanTransform(const std::shared_ptr<types::Type>& src) const override {
    switch (src->TypeId()) {
      case TypeID::kBoolean:
      case TypeID::kBinary:
      case TypeID::kDouble:
      case TypeID::kFloat:
      case TypeID::kInt:
      case TypeID::kLong:
        return true;
      default:
        return false;
    }
  }

  std::string ToString() const override {
    std::stringstream ss;
    ss << bucket_transform_prefix << "[" << num_buckets_ << "]";
    return ss.str();
  }

  std::shared_ptr<types::Type> ResultType(const std::shared_ptr<types::Type>& src) const override {
    return std::make_shared<types::PrimitiveType>(TypeID::kInt);
  }

 private:
  int num_buckets_;
};

class VoidTransform : public ITransform {
 public:
  explicit VoidTransform() {}

  std::shared_ptr<arrow::Scalar> Transform(const std::shared_ptr<arrow::Scalar>& src) override {
    return arrow::MakeNullScalar(arrow::int32());
  }

  bool CanTransform(const std::shared_ptr<types::Type>& src) const override { return true; }

  std::string ToString() const override { return void_transform_prefix; }

  std::shared_ptr<types::Type> ResultType(const std::shared_ptr<types::Type>& src) const override { return nullptr; }
};

class TruncateTransform : public ITransform {
 public:
  explicit TruncateTransform(int width) : width_(width) {}

  std::shared_ptr<arrow::Scalar> Transform(const std::shared_ptr<arrow::Scalar>& src) override {
    switch (src->type->id()) {
      case arrow::Type::BOOL:
        return TruncateNumeric<arrow::BooleanScalar>(std::static_pointer_cast<arrow::BooleanScalar>(src));
      case arrow::Type::INT8:
        return TruncateNumeric<arrow::Int8Scalar>(std::static_pointer_cast<arrow::Int8Scalar>(src));
      case arrow::Type::INT16:
        return TruncateNumeric<arrow::Int16Scalar>(std::static_pointer_cast<arrow::Int16Scalar>(src));
      case arrow::Type::INT32:
        return TruncateNumeric<arrow::Int32Scalar>(std::static_pointer_cast<arrow::Int32Scalar>(src));
      case arrow::Type::INT64:
        return TruncateNumeric<arrow::Int64Scalar>(std::static_pointer_cast<arrow::Int64Scalar>(src));
      case arrow::Type::UINT8:
        return TruncateNumeric<arrow::UInt8Scalar>(std::static_pointer_cast<arrow::UInt8Scalar>(src));
      case arrow::Type::UINT16:
        return TruncateNumeric<arrow::UInt16Scalar>(std::static_pointer_cast<arrow::UInt16Scalar>(src));
      case arrow::Type::UINT32:
        return TruncateNumeric<arrow::UInt32Scalar>(std::static_pointer_cast<arrow::UInt32Scalar>(src));
      case arrow::Type::UINT64:
        return TruncateNumeric<arrow::UInt64Scalar>(std::static_pointer_cast<arrow::UInt64Scalar>(src));
      case arrow::Type::STRING:
        return TruncateString(std::static_pointer_cast<arrow::StringScalar>(src));
      case arrow::Type::FLOAT:
        return TruncateFloat(std::static_pointer_cast<arrow::FloatScalar>(src));
      case arrow::Type::DOUBLE:
        return TruncateDouble(std::static_pointer_cast<arrow::DoubleScalar>(src));
      default:
        throw std::runtime_error("Unsupported type");
    }
  }

  bool CanTransform(const std::shared_ptr<types::Type>& src) const override {
    switch (src->TypeId()) {
      case TypeID::kBoolean:
      case TypeID::kInt:
      case TypeID::kLong:
      case TypeID::kString:
      case TypeID::kDouble:
      case TypeID::kFloat:
        return true;
      default:
        return false;
    }
  }

  std::string ToString() const override {
    std::stringstream ss;
    ss << truncate_transform_prefix << "[" << width_ << "]";
    return ss.str();
  }

  std::shared_ptr<types::Type> ResultType(const std::shared_ptr<types::Type>& src) const override {
    return std::make_shared<types::PrimitiveType>(TypeID::kInt);
  }

 private:
  int64_t ApplyTruncate(int64_t representation) const {
    return MurMurScalarHashImpl(arrow::Int64Scalar(representation)).hash_ -
           (((MurMurScalarHashImpl(arrow::Int64Scalar(representation)).hash_ % width_) + width_) % width_);
  }

  template <typename V>
  std::shared_ptr<V> TruncateNumeric(std::shared_ptr<V> value) const {
    auto cur_value =
        MurMurScalarHashImpl(*value).hash_ - (((MurMurScalarHashImpl(*value).hash_ % width_) + width_) % width_);
    return std::make_shared<V>(cur_value);
  }

  std::shared_ptr<arrow::StringScalar> TruncateString(const std::shared_ptr<arrow::StringScalar>& source) const {
    return std::make_shared<arrow::StringScalar>(source->ToString().substr(0, width_));
  }

  std::shared_ptr<arrow::FloatScalar> TruncateFloat(std::shared_ptr<arrow::FloatScalar> value) const {
    auto representation = std::bit_cast<int>(value->value);
    int result = ApplyTruncate(static_cast<int64_t>(representation));
    return std::make_shared<arrow::FloatScalar>(std::bit_cast<float>(result));
  }

  std::shared_ptr<arrow::DoubleScalar> TruncateDouble(std::shared_ptr<arrow::DoubleScalar> value) const {
    auto representation = std::bit_cast<int64_t>(value->value);
    auto result = ApplyTruncate(representation);
    return std::make_shared<arrow::DoubleScalar>(std::bit_cast<double>(result));
  }

  int width_;
};

class MonthTransform : public ITransform {
 public:
  explicit MonthTransform() = default;

  std::shared_ptr<arrow::Scalar> Transform(const std::shared_ptr<arrow::Scalar>& src) override;
  bool CanTransform(const std::shared_ptr<types::Type>& src) const override;
  std::string ToString() const override;
  std::shared_ptr<types::Type> ResultType(const std::shared_ptr<types::Type>& src) const override;
};

class YearTransform : public ITransform {
 public:
  explicit YearTransform() = default;

  std::shared_ptr<arrow::Scalar> Transform(const std::shared_ptr<arrow::Scalar>& src) override;
  bool CanTransform(const std::shared_ptr<types::Type>& src) const override;
  std::string ToString() const override;
  std::shared_ptr<types::Type> ResultType(const std::shared_ptr<types::Type>& src) const override;
};

class DayTransform : public ITransform {
 public:
  explicit DayTransform() = default;

  std::shared_ptr<arrow::Scalar> Transform(const std::shared_ptr<arrow::Scalar>& src) override;
  bool CanTransform(const std::shared_ptr<types::Type>& src) const override;
  std::string ToString() const override;
  std::shared_ptr<types::Type> ResultType(const std::shared_ptr<types::Type>& src) const override;
};

class HourTransform : public ITransform {
 public:
  explicit HourTransform() = default;

  std::shared_ptr<arrow::Scalar> Transform(const std::shared_ptr<arrow::Scalar>& src) override;
  bool CanTransform(const std::shared_ptr<types::Type>& src) const override;
  std::string ToString() const override;
  std::shared_ptr<types::Type> ResultType(const std::shared_ptr<types::Type>& src) const override;
};

std::shared_ptr<ITransform> GetTransform(const std::string& name);

}  // namespace iceberg
