#pragma once

#include <cassert>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace iceberg {

enum class TypeID {
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

namespace types {

std::optional<TypeID> NameToPrimitiveType(const std::string& name);
std::optional<std::string> PrimitiveTypeToName(TypeID type);

inline bool IsDecimalType(TypeID type) { return type == TypeID::kDecimal; }
inline bool IsBinaryType(TypeID type) { return type == TypeID::kBinary; }
inline bool IsStringType(TypeID type) { return type == TypeID::kString; }
inline bool IsFixedType(TypeID type) { return type == TypeID::kFixed; }
inline bool IsUuidType(TypeID type) { return type == TypeID::kUuid; }

class Type {
 public:
  explicit Type(TypeID id) : id_(id) {}
  virtual ~Type() = default;

  virtual bool IsListType() const { return false; }
  // virtual bool IsMapType() const { return false; }
  // virtual bool IsNestedType() const { return false; }
  virtual bool IsPrimitiveType() const { return false; }
  // virtual bool IsStructType() const { return false; }

  virtual std::string ToString() const = 0;

  TypeID TypeId() const { return id_; }

 protected:
  TypeID id_;
};

class PrimitiveType : public Type {
 public:
  explicit PrimitiveType(TypeID id) : Type(id) {}

  std::string ToString() const override {
    std::optional<std::string> result = PrimitiveTypeToName(id_).value();
    assert(result.has_value());
    return result.value();
  }

  bool IsPrimitiveType() const override { return true; }
};

class DecimalType final : public PrimitiveType {
 public:
  DecimalType(int32_t precision, int32_t scale)
      : PrimitiveType(TypeID::kDecimal), precision_(precision), scale_(scale) {
    if (precision <= 0) {
      throw std::runtime_error("DecimalType: precision = " + std::to_string(precision));
    }
    if (precision > kMaxPrecision) {
      throw std::runtime_error("DecimalType: precision = " + std::to_string(precision));
    }
  }

  std::string ToString() const override {
    return "decimal(" + std::to_string(precision_) + ", " + std::to_string(scale_) + ")";
  }

  int32_t Precision() const { return precision_; }
  int32_t Scale() const { return scale_; }

 private:
  static constexpr int32_t kMaxPrecision = 38;

  int32_t precision_;
  int32_t scale_;
};

class ListType final : public Type {
 public:
  ListType(int32_t element_id, bool element_required, std::shared_ptr<const Type> element_type)
      : Type(TypeID::kList),
        element_id_(element_id),
        element_required_(element_required),
        element_type_(std::move(element_type)) {}

  int32_t ElementId() const { return element_id_; }

  bool ElementRequired() const { return element_required_; }

  std::shared_ptr<const Type> ElementType() const { return element_type_; }

  bool IsListType() const override { return true; }
  // bool IsNestedType() const override { return true; }

  std::string ToString() const override { return "list(" + element_type_->ToString() + ")"; }

 private:
  int32_t element_id_;
  bool element_required_;
  std::shared_ptr<const Type> element_type_;
};

}  // namespace types
}  // namespace iceberg
