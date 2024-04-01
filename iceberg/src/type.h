#pragma once

#include <cassert>
#include <memory>
#include <optional>
#include <string>
#include <utility>

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
  kMap = 16,
  kUnknown = 17
};

std::optional<Type> NameToType(const std::string& name);

std::optional<std::string> TypeToName(Type type);

class DataType {
 public:
  explicit DataType(Type id) : id_(id) {}
  virtual ~DataType() = default;

  virtual bool IsDecimal() const { return false; }

  virtual bool IsPrimitive() const { return false; }

  virtual bool IsList() const { return false; }

  virtual std::string ToString() const = 0;

  Type Id() const { return id_; }

 protected:
  Type id_;
};

class PrimitiveDataType final : public DataType {
 public:
  PrimitiveDataType(Type id) : DataType(id) {}

  std::string ToString() const override {
    std::optional<std::string> result = TypeToName(id_).value();
    assert(result.has_value());
    return result.value();
  }

  bool IsPrimitive() const override { return true; }
};

class DecimalDataType final : public DataType {
 public:
  DecimalDataType(int32_t precision, int32_t scale) : DataType(Type::kDecimal), precision_(precision), scale_(scale) {
    if (precision <= 0) {
      throw std::runtime_error("PrimitiveDataType: precision = " + std::to_string(precision));
    }
    if (precision > kMaxPrecision) {
      throw std::runtime_error("PrimitiveDataType: precision = " + std::to_string(precision));
    }
  }

  std::string ToString() const override {
    return "decimal(" + std::to_string(precision_) + ", " + std::to_string(scale_) + ")";
  }

  bool IsDecimal() const override { return true; }

  int32_t Precision() const { return precision_; }
  int32_t Scale() const { return scale_; }

 private:
  static constexpr int32_t kMaxPrecision = 38;

  int32_t precision_;
  int32_t scale_;
};

class ListDataType final : public DataType {
 public:
  ListDataType(int32_t element_id, bool element_required, std::shared_ptr<const DataType> element_type)
      : DataType(Type::kList),
        element_id_(element_id),
        element_required_(element_required),
        element_type_(std::move(element_type)) {}

  int32_t ElementId() const { return element_id_; }

  bool ElementRequired() const { return element_required_; }

  std::shared_ptr<const DataType> ElementType() const { return element_type_; }

  bool IsList() const override { return true; }

  std::string ToString() const override { return "list(" + element_type_->ToString() + ")"; }

 private:
  int32_t element_id_;
  bool element_required_;
  std::shared_ptr<const DataType> element_type_;
};

}  // namespace iceberg
