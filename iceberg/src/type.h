#pragma once

#include <memory>
#include <string>

namespace iceberg {

enum class Type {
  kBoolean = 0,
  kInt = 1,
  kLong = 2,
  kFloat = 3,
  kDouble = 4,
  kDecimal = 5,
  kDate = 6,
  kTime = 7,
  kTimestamp = 8,
  kTimestamptz = 9,
  kString = 10,
  kUuid = 11,
  kFixed = 12,
  kBinary = 13,
  kStruct = 14,
  kList = 15,
  kMap = 16
};

class DataType {
 public:
  DataType(Type id) : id_(id) {}
  virtual ~DataType() = default;

  bool IsDecimal() const { return id_ == Type::kDecimal; }

 protected:
  Type id_;
};

class PrimitiveDataType final : public DataType {
 public:
  PrimitiveDataType(Type id) : DataType(id) {}
};

class DecimalDataType final : public DataType {
 public:
  DecimalDataType(int32_t precision, int32_t scale)
      : DataType(Type::kDecimal), precision_(precision), scale_(scale) {
    if (precision <= 0) {
      throw std::runtime_error("PrimitiveDataType: precision = " +
                               std::to_string(precision));
    }
    if (precision > kMaxPrecision) {
      throw std::runtime_error("PrimitiveDataType: precision = " +
                               std::to_string(precision));
    }
  }

  int32_t Precision() const { return precision_; }
  int32_t Scale() const { return scale_; }

 private:
  static constexpr int32_t kMaxPrecision = 38;

  int32_t precision_;
  int32_t scale_;
};

std::shared_ptr<const DataType> StringToDataType(const std::string& str);

}  // namespace iceberg
