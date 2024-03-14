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

  virtual bool IsDecimal() const { return false; }

  virtual bool IsPrimitive() const { return false; }

  virtual std::string ToString() const = 0;

 protected:
  Type id_;
};

class PrimitiveDataType final : public DataType {
 public:
  PrimitiveDataType(Type id) : DataType(id) {}

  bool IsPrimitive() const override { return true; }

  std::string ToString() const override;
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

  bool IsDecimal() const override { return true; }

  std::string ToString() const override;

  int32_t Precision() const { return precision_; }
  int32_t Scale() const { return scale_; }

 private:
  static constexpr int32_t kMaxPrecision = 38;

  int32_t precision_;
  int32_t scale_;
};

std::shared_ptr<const DataType> StringToDataType(const std::string& str);

std::string DataTypeToString(std::shared_ptr<const DataType> data_type);

}  // namespace iceberg
