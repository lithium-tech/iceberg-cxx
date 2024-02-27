#pragma once

#include <memory>
#include <string>

namespace iceberg {

enum class Type {
  BOOLEAN = 0,
  INT = 1,
  LONG = 2,
  FLOAT = 3,
  DOUBLE = 4,
  DECIMAL = 5,
  DATE = 6,
  TIME = 7,
  TIMESTAMP = 8,
  TIMESTAMPTZ = 9,
  STRING = 10,
  UUID = 11,
  FIXED = 12,
  BINARY = 13,
  STRUCT = 14,
  LIST = 15,
  MAP = 16
};

class DataType {
 public:
  DataType(Type id) : id_(id) {}
  virtual ~DataType() = default;

  bool IsDecimal() const { return id_ == Type::DECIMAL; }

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
      : DataType(Type::DECIMAL), precision_(precision), scale_(scale) {
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
