#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <stdexcept>

#include "arrow/array.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_decimal.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/decimal.h"
#include "gen/src/batch.h"
#include "gen/src/list.h"

namespace gen {

class RandomDevice {
 public:
  using result_type = uint64_t;
  static constexpr result_type min() { return std::numeric_limits<result_type>::min(); }
  static constexpr result_type max() { return std::numeric_limits<result_type>::max(); }

  RandomDevice(int64_t seed) : rng_(seed) {}

  uint64_t operator()() { return rng_(); }

 private:
  std::mt19937_64 rng_;
};

class UniformInt64Distribution {
 public:
  UniformInt64Distribution(int64_t min_value, int64_t max_value) : uid_(min_value, max_value) {
    if (max_value < min_value) {
      throw std::runtime_error("UniformInt64Distribution: max_value must be greater than min_value");
    }
  }

  int64_t operator()(RandomDevice& random_device) { return uid_(random_device); }

 private:
  std::uniform_int_distribution<> uid_;
};

class BernouliDistribution {
 public:
  BernouliDistribution(double p) : uid_(p) {}

  int64_t operator()(RandomDevice& random_device) { return uid_(random_device); }

 private:
  std::bernoulli_distribution uid_;
};

using ArrayPtr = std::shared_ptr<arrow::Array>;

class Generator {
 public:
  explicit Generator(const arrow::FieldVector& fields) {
    names_.reserve(fields.size());
    types_.reserve(fields.size());
    for (size_t i = 0; i < fields.size(); ++i) {
      names_.emplace_back(fields[i]->name());
      types_.emplace_back(fields[i]->type());
    }
  }

  arrow::Result<BatchPtr> PrepareBatch(BatchPtr batch) const {
    ARROW_ASSIGN_OR_RAISE(batch, batch->GetProjection(names_));

    for (size_t i = 0; i < types_.size(); ++i) {
      if (!batch->Column(i)->type()->Equals(types_[i])) {
        return arrow::Status::ExecutionError("Expected type for column ", names_[i], " is ", types_[i]->ToString(),
                                             " found ", batch->Column(i)->type());
      }
    }

    return batch;
  }

  Generator() = default;

  virtual arrow::Result<ArrayPtr> Generate(BatchPtr) = 0;

  virtual ~Generator() = default;

 private:
  std::vector<std::string> names_;
  std::vector<std::shared_ptr<arrow::DataType>> types_;
};

namespace internal {

template <typename ArrowType>
class ArrowTrait {
 public:
  using ValueType = typename ArrowType::c_type;
  using BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;
};

template <>
class ArrowTrait<arrow::StringType> {
 public:
  using ValueType = std::string;
  using BuilderType = arrow::StringBuilder;
};

}  // namespace internal

template <typename ArrowType>
class TypedGenerator : public Generator {
 public:
  using ValueType = typename internal::ArrowTrait<ArrowType>::ValueType;
  using BuilderType = typename internal::ArrowTrait<ArrowType>::BuilderType;

  TypedGenerator() = default;

  TypedGenerator(const arrow::FieldVector& fields) : Generator(fields) {}

  virtual arrow::Result<ArrayPtr> Generate(BatchPtr batch) final {
    ARROW_ASSIGN_OR_RAISE(batch, PrepareBatch(batch));

    BuilderType builder;
    ARROW_ASSIGN_OR_RAISE(auto values, GenerateValues(batch));
    ARROW_RETURN_NOT_OK(builder.AppendValues(values));
    return builder.Finish();
  }

  virtual arrow::Result<std::vector<ValueType>> GenerateValues(BatchPtr) = 0;

  virtual ~TypedGenerator() = default;
};

using Int32Generator = TypedGenerator<arrow::Int32Type>;
using Int64Generator = TypedGenerator<arrow::Int64Type>;
using StringGenerator = TypedGenerator<arrow::StringType>;

template <typename ArrowType>
class TrivialGenerator : public TypedGenerator<ArrowType> {
 public:
  using ValueType = typename TypedGenerator<ArrowType>::ValueType;

  virtual arrow::Result<std::vector<ValueType>> GenerateValues(BatchPtr batch) final {
    std::vector<ValueType> result;
    result.reserve(batch->NumRows());
    for (int64_t i = 0; i < batch->NumRows(); ++i) {
      result.emplace_back(GenerateValue());
    }

    return result;
  }

  virtual ValueType GenerateValue() = 0;
};

using TrivialInt32Generator = TrivialGenerator<arrow::Int32Type>;
using TrivialInt64Generator = TrivialGenerator<arrow::Int64Type>;
using TrivialStringGenerator = TrivialGenerator<arrow::StringType>;

template <typename ArrowType>
class WithArgsGenerator : public TypedGenerator<ArrowType> {
 public:
  using ValueType = typename TypedGenerator<ArrowType>::ValueType;

  WithArgsGenerator(const arrow::FieldVector& fields) : TypedGenerator<ArrowType>(fields) {}

  virtual arrow::Result<std::vector<ValueType>> GenerateValues(BatchPtr batch) final {
    std::vector<ValueType> result;
    result.reserve(batch->NumRows());
    for (int64_t i = 0; i < batch->NumRows(); ++i) {
      result.emplace_back(GenerateValue(batch, i));
    }

    return result;
  }

  virtual ValueType GenerateValue(BatchPtr batch, uint64_t row_index) = 0;
};

using WithArgsInt32Generator = WithArgsGenerator<arrow::Int32Type>;
using WithArgsInt64Generator = WithArgsGenerator<arrow::Int64Type>;
using WithArgsStringGenerator = WithArgsGenerator<arrow::StringType>;

// TODO(gmusya): optimize
class StringFromListGenerator : public TrivialStringGenerator {
 public:
  StringFromListGenerator(const List& list, RandomDevice& random_device);

  std::string GenerateValue() override {
    int64_t weight = generator_(random_device_);
    auto position =
        std::upper_bound(cumulative_weights_.begin(), cumulative_weights_.end(), weight) - cumulative_weights_.begin();
    return values_[position];
  }

 private:
  RandomDevice& random_device_;
  std::vector<std::string> values_;
  std::vector<int64_t> cumulative_weights_;
  UniformInt64Distribution generator_;
};

template <typename ArrowType>
class ConstantGenerator : public TrivialGenerator<ArrowType> {
 public:
  using ValueType = typename TrivialGenerator<ArrowType>::ValueType;

  ConstantGenerator(ValueType value) : value_(value) {}

  ValueType GenerateValue() override { return value_; }

 private:
  const ValueType value_;
};

class RepetitionLevelsGenerator : public Int32Generator {
 public:
  RepetitionLevelsGenerator(const std::string& field_name)
      : Int32Generator({std::make_shared<arrow::Field>(field_name, arrow::int32())}) {}

  arrow::Result<std::vector<int32_t>> GenerateValues(BatchPtr record_batch) override {
    auto column = std::static_pointer_cast<arrow::Int32Array>(record_batch->Column(0));
    std::vector<int32_t> result;

    // TODO(gmusya): add reserve
    for (int64_t i = 0; i < record_batch->NumRows(); ++i) {
      int32_t size = column->Value(i);
      if (size < 1) {
        return arrow::Status::ExecutionError("Expected size >= 1, got ", size);
      }
      result.emplace_back(0);
      for (int32_t j = 1; j < size; ++j) {
        result.emplace_back(1);
      }
    }

    return result;
  }
};

template <typename InputArrowType>
class ToStringGenerator : public WithArgsStringGenerator {
 public:
  using InputArrayType = arrow::NumericArray<InputArrowType>;
  using InputValueType = typename InputArrowType::c_type;

  static_assert(std::is_same_v<arrow::Int32Type, InputArrowType> || std::is_same_v<arrow::Int64Type, InputArrowType>);

  ToStringGenerator(std::string_view arg_name)
      : WithArgsStringGenerator(
            {std::make_shared<arrow::Field>(std::string(arg_name), std::make_shared<InputArrowType>())}) {}

  std::string GenerateValue(BatchPtr record_batch, uint64_t row_index) override {
    auto column = std::static_pointer_cast<InputArrayType>(record_batch->Column(0));
    return std::to_string(column->GetView(row_index));
  }
};

template <typename InputArrowType>
class ToDecimalGenerator : public Generator {
  using InputArrayType = arrow::NumericArray<InputArrowType>;

 public:
  ToDecimalGenerator(const std::string& field_name, int32_t precision, int32_t scale)
      : Generator({std::make_shared<arrow::Field>(field_name, std::make_shared<InputArrowType>())}),
        precision_(precision),
        scale_(scale) {}

  virtual arrow::Result<ArrayPtr> Generate(BatchPtr batch) {
    ARROW_ASSIGN_OR_RAISE(batch, PrepareBatch(batch));

    auto column = std::static_pointer_cast<InputArrayType>(batch->Column(0));

    arrow::Decimal128Builder builder(arrow::decimal(precision_, scale_));
    ARROW_RETURN_NOT_OK(builder.Reserve(batch->NumRows()));

    for (int64_t i = 0; i < batch->NumRows(); ++i) {
      auto val = column->GetView(i);
      ARROW_ASSIGN_OR_RAISE(auto dec, arrow::Decimal128::FromReal(static_cast<double>(val), precision_, 0));
      ARROW_RETURN_NOT_OK(builder.Append(dec));
    }

    return builder.Finish();
  }

 private:
  const int32_t precision_;
  const int32_t scale_;
};

class ConcatenateGenerator : public WithArgsStringGenerator {
 public:
  ConcatenateGenerator(const std::vector<std::string>& names, const std::string& delimiter)
      : WithArgsStringGenerator([&]() {
          std::vector<std::shared_ptr<arrow::Field>> fields;
          fields.reserve(names.size());
          for (size_t i = 0; i < names.size(); ++i) {
            fields.emplace_back(std::make_shared<arrow::Field>(names[i], arrow::utf8()));
          }
          return fields;
        }()),
        delimiter_(delimiter) {}

  std::string GenerateValue(BatchPtr record_batch, uint64_t row_index) override {
    std::string result;

    for (int32_t col_id = 0; col_id < record_batch->NumColumns(); ++col_id) {
      if (col_id != 0) {
        result += delimiter_;
      }
      result += std::static_pointer_cast<arrow::StringArray>(record_batch->Column(col_id))->GetString(row_index);
    }
    return result;
  }

 private:
  std::string delimiter_;
};

template <typename ArrowType>
class UniqueIntegerGenerator : public TrivialGenerator<ArrowType> {
  using ValueType = typename TrivialGenerator<ArrowType>::ValueType;

 public:
  UniqueIntegerGenerator() {}

  ValueType GenerateValue() override { return static_cast<ValueType>(last_value_++); }

 private:
  int64_t last_value_ = 1;
};

template <typename ArrowType>
class FromArrayGenerator : public TrivialGenerator<ArrowType> {
  using ValueType = typename TrivialGenerator<ArrowType>::ValueType;

 public:
  FromArrayGenerator(const std::vector<ValueType>& values) : values_(values) {}

  ValueType GenerateValue() override {
    if (last_index_ >= values_.size()) {
      throw std::runtime_error("FromArrayGenerator: index out of range");
    }
    return values_[last_index_++];
  }

 private:
  const std::vector<ValueType> values_;
  uint64_t last_index_ = 0;
};

template <typename ArrowType>
class BernouliGenerator : public TrivialGenerator<ArrowType> {
 public:
  using ValueType = typename TrivialGenerator<ArrowType>::ValueType;

 public:
  BernouliGenerator(double p, RandomDevice& random_device) : random_device_(random_device), generator_(p) {}

  ValueType GenerateValue() override { return static_cast<ValueType>(generator_(random_device_)); }

 private:
  RandomDevice& random_device_;
  BernouliDistribution generator_;
};

template <typename ArrowType>
class UniformIntegerGenerator : public TrivialGenerator<ArrowType> {
 public:
  using ValueType = typename TrivialGenerator<ArrowType>::ValueType;

  static_assert(std::is_same_v<int32_t, ValueType> || std::is_same_v<int64_t, ValueType>);

 public:
  UniformIntegerGenerator(int64_t min_value, int64_t max_value, RandomDevice& random_device)
      : random_device_(random_device), generator_(min_value, max_value) {}

  ValueType GenerateValue() override { return static_cast<ValueType>(generator_(random_device_)); }

 private:
  RandomDevice& random_device_;
  UniformInt64Distribution generator_;
};

template <typename OutputArrowType, typename LhsArrowType, typename RhsArrowType>
class MultiplyGenerator : public WithArgsGenerator<OutputArrowType> {
  using ValueType = typename WithArgsGenerator<OutputArrowType>::ValueType;
  using LhsArrowArray = arrow::NumericArray<LhsArrowType>;
  using RhsArrowArray = arrow::NumericArray<RhsArrowType>;

 public:
  MultiplyGenerator(std::string_view lhs_name, std::string_view rhs_name)
      : WithArgsGenerator<OutputArrowType>(
            {std::make_shared<arrow::Field>(std::string(lhs_name), std::make_shared<LhsArrowType>()),
             std::make_shared<arrow::Field>(std::string(rhs_name), std::make_shared<RhsArrowType>())}) {}

  ValueType GenerateValue(BatchPtr record_batch, uint64_t row_index) override {
    auto lhs_column = std::static_pointer_cast<LhsArrowArray>(record_batch->Column(0));
    auto rhs_column = std::static_pointer_cast<RhsArrowArray>(record_batch->Column(1));
    return lhs_column->GetView(row_index) * rhs_column->GetView(row_index);
  }
};

template <typename OutputArrowType, typename LhsArrowType, typename RhsArrowType>
class AddGenerator : public WithArgsGenerator<OutputArrowType> {
  using ValueType = typename WithArgsGenerator<OutputArrowType>::ValueType;
  using LhsArrowArray = arrow::NumericArray<LhsArrowType>;
  using RhsArrowArray = arrow::NumericArray<RhsArrowType>;

 public:
  AddGenerator(std::string_view lhs_name, std::string_view rhs_name)
      : WithArgsGenerator<OutputArrowType>(
            {std::make_shared<arrow::Field>(std::string(lhs_name), std::make_shared<LhsArrowType>()),
             std::make_shared<arrow::Field>(std::string(rhs_name), std::make_shared<RhsArrowType>())}) {}

  ValueType GenerateValue(BatchPtr record_batch, uint64_t row_index) override {
    auto lhs_column = std::static_pointer_cast<LhsArrowArray>(record_batch->Column(0));
    auto rhs_column = std::static_pointer_cast<RhsArrowArray>(record_batch->Column(1));
    return lhs_column->GetView(row_index) + rhs_column->GetView(row_index);
  }
};

template <typename ArrowType>
class CopyGenerator : public TypedGenerator<ArrowType> {
  using ValueType = typename TypedGenerator<ArrowType>::ValueType;
  using ArrayType = arrow::NumericArray<ArrowType>;

 public:
  CopyGenerator(std::string_view levels_name, std::string_view values_name)
      : TypedGenerator<ArrowType>(
            {std::make_shared<arrow::Field>(std::string(levels_name), arrow::int32()),
             std::make_shared<arrow::Field>(std::string(values_name), std::make_shared<ArrowType>())}) {}

  arrow::Result<std::vector<ValueType>> GenerateValues(BatchPtr record_batch) override {
    auto levels_column = std::static_pointer_cast<arrow::Int32Array>(record_batch->Column(0));
    auto values_column = std::static_pointer_cast<arrow::Int32Array>(record_batch->Column(1));

    std::vector<ValueType> result;
    result.reserve(levels_column->length());

    int32_t current_value_index = -1;

    for (int64_t i = 0; i < levels_column->length(); ++i) {
      current_value_index += levels_column->GetView(i) == 0;
      result.emplace_back(values_column->GetView(current_value_index));
    }

    return result;
  }
};

template <typename ArrowType>
class AggregatorGenerator : public TypedGenerator<ArrowType> {
 public:
  using ValueType = typename TypedGenerator<ArrowType>::ValueType;

  AggregatorGenerator(const arrow::FieldVector& fields) : TypedGenerator<ArrowType>(fields) {}

  arrow::Result<std::vector<ValueType>> GenerateValues(BatchPtr record_batch) override {
    auto levels_column = std::static_pointer_cast<arrow::Int32Array>(record_batch->Column(0));
    std::vector<ValueType> result;

    ARROW_ASSIGN_OR_RAISE(BatchPtr args, record_batch->DropColumnByIndex(0));

    for (int64_t i = 0; i < levels_column->length(); ++i) {
      int64_t j = i + 1;
      while (j < levels_column->length() && levels_column->GetView(j) != 0) {
        ++j;
      }

      ARROW_RETURN_NOT_OK(HandleArray(args, i, j, result));
      i = j - 1;
    }

    return result;
  }

  virtual arrow::Status HandleArray(BatchPtr args, int64_t from, int64_t to, std::vector<ValueType>& result) = 0;
};

class PositionWithinArrayGenerator : public AggregatorGenerator<arrow::Int32Type> {
 public:
  PositionWithinArrayGenerator(const std::string& arg_name, int32_t first_index)
      : AggregatorGenerator<arrow::Int32Type>({std::make_shared<arrow::Field>(arg_name, arrow::int32())}),
        first_index_(first_index) {}

  arrow::Status HandleArray(BatchPtr args, int64_t from, int64_t to, std::vector<ValueType>& result) override {
    for (int k = from; k < to; ++k) {
      int32_t value = k - from + first_index_;
      result.emplace_back(value);
    }

    return arrow::Status::OK();
  }

 private:
  const int32_t first_index_;
};

class UniqueWithinArrayGenerator : public AggregatorGenerator<arrow::Int32Type> {
 public:
  UniqueWithinArrayGenerator(const std::string& arg_name, int32_t min_value, int32_t max_value,
                             RandomDevice& random_device)
      : AggregatorGenerator<arrow::Int32Type>({std::make_shared<arrow::Field>(arg_name, arrow::int32())}),
        max_value_(max_value),
        random_device_(random_device),
        generator_(min_value, max_value) {}

  arrow::Status HandleArray(BatchPtr args, int64_t from, int64_t to, std::vector<ValueType>& result) override {
    std::vector<bool> used_values(max_value_ + 1);
    for (int k = from; k < to; ++k) {
      int32_t value = GenerateUnusedValue(used_values);
      used_values[value] = true;
      result.emplace_back(value);
    }

    return arrow::Status::OK();
  }

  int32_t GenerateUnusedValue(const std::vector<bool>& used_values) {
    while (true) {
      int32_t value = generator_(random_device_);
      if (!used_values[value]) {
        return value;
      }
    }
  }

 private:
  const int32_t max_value_;
  RandomDevice& random_device_;
  UniformInt64Distribution generator_;
};

}  // namespace gen
