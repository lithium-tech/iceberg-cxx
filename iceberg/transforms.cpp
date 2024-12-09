#include "iceberg/transforms.h"

#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include "iceberg/type.h"

namespace iceberg {

std::string PartitionKey::ToPath() const {
  std::string result;
  for (auto elem : partition_values_) {
    result += elem->ToString() + "-";
  }
  result.pop_back();
  return result;
}

bool PartitionKey::operator==(const PartitionKey& other) const {
  if (partition_values_.size() != other.partition_values_.size()) {
    return false;
  }

  for (size_t i = 0; i < partition_values_.size(); ++i) {
    if (!partition_values_[i]->Equals(*other.partition_values_[i])) {
      return false;
    }
  }
  return true;
}

uint64_t PartitionKeyHasher::operator()(const PartitionKey& key) const {
  uint64_t result = 0;

  for (const auto& value : key.partition_values_) {
    result ^= value->hash() + (result << 2) ^ (result >> 4);
  }
  return result;
}

namespace {

class TransformParser {
 public:
  void BuildParser(const std::string& current_transform) { current_transform_ = current_transform; }

  TransformType GetTransformType() const {
    if (current_transform_.substr(0, std::strlen(identity_transform_prefix)) == identity_transform_prefix) {
      return TransformType::kIdentityTransform;
    }
    if (current_transform_.substr(0, std::strlen(void_transform_prefix)) == void_transform_prefix) {
      return TransformType::kVoidTransform;
    }
    if (current_transform_.substr(0, std::strlen(bucket_transform_prefix)) == bucket_transform_prefix) {
      return TransformType::kBucketTransform;
    }
    if (current_transform_.substr(0, std::strlen(truncate_transform_prefix)) == truncate_transform_prefix) {
      return TransformType::kTruncateTransform;
    }
    if (current_transform_.substr(0, std::strlen(year_transform_prefix)) == year_transform_prefix) {
      return TransformType::kYearTransform;
    }
    if (current_transform_.substr(0, std::strlen(day_transform_prefix)) == day_transform_prefix) {
      return TransformType::kDayTransform;
    }
    if (current_transform_.substr(0, std::strlen(month_transform_prefix)) == month_transform_prefix) {
      return TransformType::kMonthTransform;
    }
    if (current_transform_.substr(0, std::strlen(hour_transform_prefix)) == hour_transform_prefix) {
      return TransformType::kHourTransform;
    }
    throw std::runtime_error("Unknown transform type " + current_transform_);
  }

  std::optional<int> GetTransformParam() const {
    auto prefix_size = GetTransformNameByType(GetTransformType()).size();
    if (current_transform_.size() == prefix_size) {
      return std::nullopt;
    }
    return std::stoi(current_transform_.substr(prefix_size + 1, current_transform_.size() - 2 - prefix_size));
  }

 private:
  std::string current_transform_;
};

}  // namespace

std::string GetTransformNameByType(TransformType type) {
  switch (type) {
    case TransformType::kBucketTransform:
      return bucket_transform_prefix;
    case TransformType::kDayTransform:
      return day_transform_prefix;
    case TransformType::kHourTransform:
      return hour_transform_prefix;
    case TransformType::kIdentityTransform:
      return identity_transform_prefix;
    case TransformType::kTruncateTransform:
      return truncate_transform_prefix;
    case TransformType::kVoidTransform:
      return void_transform_prefix;
    case TransformType::kYearTransform:
      return year_transform_prefix;
    case TransformType::kMonthTransform:
      return month_transform_prefix;
  }
  throw std::runtime_error("unknown transform type");
}

std::shared_ptr<arrow::Scalar> MonthTransform::Transform(const std::shared_ptr<arrow::Scalar>& src) {
  std::lock_guard lock(mutex_);

  int64_t millis_since_epoch = std::static_pointer_cast<arrow::Date64Scalar>(src)->value;

  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>(
      std::chrono::milliseconds(millis_since_epoch));
  std::time_t time = std::chrono::system_clock::to_time_t(tp);
  std::tm* date_tm = std::gmtime(&time);
  int month = date_tm->tm_mon + 1;
  return std::make_shared<arrow::Int32Scalar>(month);
}

bool MonthTransform::CanTransform(const std::shared_ptr<types::Type>& src) const {
  return src->TypeId() == TypeID::kDate;
}

std::string MonthTransform::ToString() const { return month_transform_prefix; }

std::shared_ptr<types::Type> MonthTransform::ResultType(const std::shared_ptr<types::Type>& src) const {
  return std::make_shared<types::PrimitiveType>(TypeID::kDate);
}

std::shared_ptr<arrow::Scalar> YearTransform::Transform(const std::shared_ptr<arrow::Scalar>& src) {
  std::lock_guard lock(mutex_);

  int64_t millis_since_epoch = std::static_pointer_cast<arrow::Date64Scalar>(src)->value;

  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>(
      std::chrono::milliseconds(millis_since_epoch));
  std::time_t time = std::chrono::system_clock::to_time_t(tp);
  std::tm* date_tm = std::gmtime(&time);
  int year = date_tm->tm_year + 1900;
  return std::make_shared<arrow::Int32Scalar>(year);
}

bool YearTransform::CanTransform(const std::shared_ptr<types::Type>& src) const {
  return src->TypeId() == TypeID::kDate;
}

std::string YearTransform::ToString() const { return year_transform_prefix; }

std::shared_ptr<types::Type> YearTransform::ResultType(const std::shared_ptr<types::Type>& src) const {
  return std::make_shared<types::PrimitiveType>(TypeID::kDate);
}

std::shared_ptr<arrow::Scalar> DayTransform::Transform(const std::shared_ptr<arrow::Scalar>& src) {
  std::lock_guard lock(mutex_);

  int64_t millis_since_epoch = std::static_pointer_cast<arrow::Date64Scalar>(src)->value;

  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>(
      std::chrono::milliseconds(millis_since_epoch));
  std::time_t time = std::chrono::system_clock::to_time_t(tp);
  std::tm* date_tm = std::gmtime(&time);
  int day = date_tm->tm_mday;
  return std::make_shared<arrow::Int32Scalar>(day);
}

bool DayTransform::CanTransform(const std::shared_ptr<types::Type>& src) const {
  return src->TypeId() == TypeID::kDate;
}

std::string DayTransform::ToString() const { return day_transform_prefix; }

std::shared_ptr<types::Type> DayTransform::ResultType(const std::shared_ptr<types::Type>& src) const {
  return std::make_shared<types::PrimitiveType>(TypeID::kDate);
}

std::shared_ptr<arrow::Scalar> HourTransform::Transform(const std::shared_ptr<arrow::Scalar>& src) {
  std::lock_guard lock(mutex_);

  int64_t millis_since_epoch = std::static_pointer_cast<arrow::Date64Scalar>(src)->value;

  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>(
      std::chrono::milliseconds(millis_since_epoch));
  std::time_t time = std::chrono::system_clock::to_time_t(tp);
  std::tm* date_tm = std::gmtime(&time);
  return std::make_shared<arrow::Int32Scalar>(date_tm->tm_hour);
}

bool HourTransform::CanTransform(const std::shared_ptr<types::Type>& src) const {
  return src->TypeId() == TypeID::kDate;
}

std::string HourTransform::ToString() const { return hour_transform_prefix; }

std::shared_ptr<types::Type> HourTransform::ResultType(const std::shared_ptr<types::Type>& src) const {
  return std::make_shared<types::PrimitiveType>(TypeID::kDate);
}

std::shared_ptr<ITransform> GetTransform(const std::string& name) {
  auto parser = TransformParser();
  parser.BuildParser(name);

  switch (parser.GetTransformType()) {
    case TransformType::kIdentityTransform: {
      return std::make_shared<IdentityTransform>();
    }
    case TransformType::kVoidTransform: {
      return std::make_shared<VoidTransform>();
    }
    case TransformType::kBucketTransform: {
      int num_buckets = *parser.GetTransformParam();
      return std::make_shared<BucketTransform>(num_buckets);
    }
    case TransformType::kTruncateTransform: {
      int truncate_length = *parser.GetTransformParam();
      return std::make_shared<TruncateTransform>(truncate_length);
    }
    case TransformType::kMonthTransform: {
      return std::make_shared<MonthTransform>();
    }
    case TransformType::kYearTransform: {
      return std::make_shared<YearTransform>();
    }
    case TransformType::kHourTransform: {
      return std::make_shared<HourTransform>();
    }
    case TransformType::kDayTransform: {
      return std::make_shared<HourTransform>();
    }
  }
  return nullptr;
}

}  // namespace iceberg
