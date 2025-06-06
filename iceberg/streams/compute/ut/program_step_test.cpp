#include <array>

#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include <arrow/type_fwd.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "iceberg/streams/compute/ssa_program.h"

namespace iceberg::compute {
namespace {

template <typename Type>
struct TypeWrapper {
  using T = Type;
};

template <typename Func, bool enable_null = false>
bool SwitchType(arrow::Type::type type_id, Func&& f) {
  switch (type_id) {
    case arrow::Type::NA: {
      if constexpr (enable_null) {
        return f(TypeWrapper<arrow::NullType>());
      }
      break;
    }
    case arrow::Type::BOOL:
      return f(TypeWrapper<arrow::BooleanType>());
    case arrow::Type::UINT8:
      return f(TypeWrapper<arrow::UInt8Type>());
    case arrow::Type::INT8:
      return f(TypeWrapper<arrow::Int8Type>());
    case arrow::Type::UINT16:
      return f(TypeWrapper<arrow::UInt16Type>());
    case arrow::Type::INT16:
      return f(TypeWrapper<arrow::Int16Type>());
    case arrow::Type::UINT32:
      return f(TypeWrapper<arrow::UInt32Type>());
    case arrow::Type::INT32:
      return f(TypeWrapper<arrow::Int32Type>());
    case arrow::Type::UINT64:
      return f(TypeWrapper<arrow::UInt64Type>());
    case arrow::Type::INT64:
      return f(TypeWrapper<arrow::Int64Type>());
    case arrow::Type::HALF_FLOAT:
      return f(TypeWrapper<arrow::HalfFloatType>());
    case arrow::Type::FLOAT:
      return f(TypeWrapper<arrow::FloatType>());
    case arrow::Type::DOUBLE:
      return f(TypeWrapper<arrow::DoubleType>());
    case arrow::Type::STRING:
      return f(TypeWrapper<arrow::StringType>());
    case arrow::Type::BINARY:
      return f(TypeWrapper<arrow::BinaryType>());
    case arrow::Type::FIXED_SIZE_BINARY:
      return f(TypeWrapper<arrow::FixedSizeBinaryType>());
    case arrow::Type::DATE32:
      return f(TypeWrapper<arrow::Date32Type>());
    case arrow::Type::DATE64:
      return f(TypeWrapper<arrow::Date64Type>());
    case arrow::Type::TIMESTAMP:
      return f(TypeWrapper<arrow::TimestampType>());
    case arrow::Type::TIME32:
      return f(TypeWrapper<arrow::Time32Type>());
    case arrow::Type::TIME64:
      return f(TypeWrapper<arrow::Time64Type>());
    case arrow::Type::INTERVAL_MONTHS:
      return f(TypeWrapper<arrow::MonthIntervalType>());
    case arrow::Type::DECIMAL:
      return f(TypeWrapper<arrow::Decimal128Type>());
    case arrow::Type::DURATION:
      return f(TypeWrapper<arrow::DurationType>());
    case arrow::Type::LARGE_STRING:
      return f(TypeWrapper<arrow::LargeStringType>());
    case arrow::Type::LARGE_BINARY:
      return f(TypeWrapper<arrow::LargeBinaryType>());
    case arrow::Type::DECIMAL256:
    case arrow::Type::DENSE_UNION:
    case arrow::Type::DICTIONARY:
    case arrow::Type::EXTENSION:
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::INTERVAL_DAY_TIME:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::LIST:
    case arrow::Type::MAP:
    case arrow::Type::MAX_ID:
    case arrow::Type::SPARSE_UNION:
    case arrow::Type::STRUCT:
    case arrow::Type::INTERVAL_MONTH_DAY_NANO:
    case arrow::Type::RUN_END_ENCODED:
    case arrow::Type::STRING_VIEW:
    case arrow::Type::BINARY_VIEW:
    case arrow::Type::LIST_VIEW:
    case arrow::Type::LARGE_LIST_VIEW:
      break;
  }

  return false;
}

template <typename ValueT>
std::shared_ptr<arrow::Array> NumVecToArray(const std::shared_ptr<arrow::DataType>& type,
                                            const std::vector<ValueT>& vec, std::optional<ValueT> null_value = {}) {
  std::shared_ptr<arrow::Array> out;
  SwitchType(type->id(), [&](const auto& t) {
    using Wrap = std::decay_t<decltype(t)>;
    using T = typename Wrap::T;

    if constexpr (arrow::is_number_type<T>::value) {
      typename arrow::TypeTraits<T>::BuilderType builder;
      for (const auto val : vec) {
        if (null_value && *null_value == val) {
          builder.AppendNull().ok();
        } else {
          builder.Append(static_cast<typename T::c_type>(val)).ok();
        }
      }
      builder.Finish(&out).ok();
      return true;
    } else if constexpr (arrow::is_timestamp_type<T>()) {
      typename arrow::TypeTraits<T>::BuilderType builder(type, arrow::default_memory_pool());
      for (const auto val : vec) {
        builder.Append(static_cast<typename T::c_type>(val)).ok();
      }
      builder.Finish(&out).ok();
      return true;
    }
    return false;
  });
  return out;
}

std::shared_ptr<arrow::Array> BoolVecToArray(const std::vector<bool>& vec) {
  std::shared_ptr<arrow::Array> out;
  arrow::BooleanBuilder builder;
  for (const auto val : vec) {
    builder.Append(val).ok();
  }
  builder.Finish(&out).ok();
  return out;
}

size_t FilterTest(std::vector<std::shared_ptr<arrow::Array>> args, SsaOperation op1, SsaOperation op2) {
  auto schema = std::make_shared<arrow::Schema>(std::vector{std::make_shared<arrow::Field>("x", args.at(0)->type()),
                                                            std::make_shared<arrow::Field>("y", args.at(1)->type()),
                                                            std::make_shared<arrow::Field>("z", args.at(2)->type())});
  auto batch = arrow::RecordBatch::Make(schema, 3, std::vector{args.at(0), args.at(1), args.at(2)});
  EXPECT_TRUE(batch->ValidateFull().ok());

  const std::string res1 = "res1";
  const std::string res2 = "res2";
  const std::string x = "x";
  const std::string y = "y";
  const std::string z = "z";

  auto step = std::make_shared<ProgramStep>();
  step->assignes.emplace_back(Assign(res1, op1, {x, y}));
  step->assignes.emplace_back(Assign(res2, op2, {res1, z}));
  step->filters.emplace_back(res2);
  step->projection.emplace_back(res1);
  step->projection.emplace_back(res2);

  auto status = ApplyProgram(batch, Program({step}));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
  }
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(batch->ValidateFull().ok());
  EXPECT_EQ(batch->num_columns(), 2);
  return batch->num_rows();
}

size_t FilterTestUnary(std::vector<std::shared_ptr<arrow::Array>> args, SsaOperation op1, SsaOperation op2) {
  auto schema = std::make_shared<arrow::Schema>(std::vector{std::make_shared<arrow::Field>("x", args.at(0)->type()),
                                                            std::make_shared<arrow::Field>("z", args.at(1)->type())});
  auto batch = arrow::RecordBatch::Make(schema, 3, std::vector{args.at(0), args.at(1)});
  EXPECT_TRUE(batch->ValidateFull().ok());

  const std::string res1 = "res1";
  const std::string res2 = "res2";
  const std::string x = "x";
  const std::string z = "z";

  auto step = std::make_shared<ProgramStep>();
  step->assignes.emplace_back(Assign(res1, op1, {x}));
  step->assignes.emplace_back(Assign(res2, op2, {res1, z}));
  step->filters.emplace_back(res2);
  step->projection.emplace_back(res1);
  step->projection.emplace_back(res2);

  auto status = ApplyProgram(batch, Program({step}));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
  }
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(batch->ValidateFull().ok());
  EXPECT_EQ(batch->num_columns(), 2);
  return batch->num_rows();
}

std::vector<bool> LikeTest(const std::vector<std::string>& data, SsaOperation op, const std::string& pattern,
                           std::shared_ptr<arrow::DataType> type = arrow::utf8(), bool ignore_case = false) {
  auto schema = std::make_shared<arrow::Schema>(std::vector{std::make_shared<arrow::Field>("x", type)});
  std::shared_ptr<arrow::RecordBatch> batch;
  if (type->id() == arrow::utf8()->id()) {
    arrow::StringBuilder sb;
    sb.AppendValues(data).ok();
    batch = arrow::RecordBatch::Make(schema, data.size(), {*sb.Finish()});
  } else if (type->id() == arrow::binary()->id()) {
    arrow::BinaryBuilder sb;
    sb.AppendValues(data).ok();
    batch = arrow::RecordBatch::Make(schema, data.size(), {*sb.Finish()});
  }
  EXPECT_TRUE(batch->ValidateFull().ok());

  const std::string x = "x";
  const std::string res = "res";

  auto step = std::make_shared<ProgramStep>();
  step->assignes.emplace_back(
      Assign(res, op, {x}, std::make_shared<arrow::compute::MatchSubstringOptions>(pattern, ignore_case)));
  step->projection.emplace_back(res);

  auto status = ApplyProgram(batch, Program({step}));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
  }
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(batch->ValidateFull().ok());
  EXPECT_EQ(batch->num_columns(), 1);

  auto& res_column = static_cast<const arrow::BooleanArray&>(*batch->GetColumnByName(res));
  std::vector<bool> vec;
  for (int i = 0; i < res_column.length(); ++i) {
    EXPECT_TRUE(!res_column.IsNull(i));
    vec.push_back(res_column.Value(i));
  }
  return vec;
}

}  // namespace

TEST(ProgramStep, Round0) {
  for (auto eop : {SsaOperation::kRound}) {  // TODO: RoundBankers
    auto x = NumVecToArray<float>(arrow::float64(), {32.3, 12.5, 34.7});
    auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x});
    EXPECT_TRUE(FilterTestUnary({x, z->make_array()}, eop, SsaOperation::kEqual) == 3);
  }
}

TEST(ProgramStep, Round1) {
  for (auto eop : {SsaOperation::kCeil, SsaOperation::kFloor}) {
    auto x = NumVecToArray<float>(arrow::float64(), {32.3, 12.5, 34.7});
    auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x});
    EXPECT_TRUE(FilterTestUnary({x, z->make_array()}, eop, SsaOperation::kEqual) == 3);
  }
}

TEST(ProgramStep, Filter) {
  auto x = NumVecToArray<int>(arrow::int32(), {10, 34, 8});
  auto y = NumVecToArray<int>(arrow::uint32(), {10, 34, 8});
  auto z = NumVecToArray<int>(arrow::int64(), {33, 70, 12});
  EXPECT_TRUE(FilterTest({x, y, z}, SsaOperation::kAddWithoutChecks, SsaOperation::kLessThan) == 2);
}

TEST(ProgramStep, Add) {
  auto x = NumVecToArray<int>(arrow::int32(), {10, 34, 8});
  auto y = NumVecToArray<int>(arrow::int32(), {32, 12, 4});
  auto z = arrow::compute::CallFunction("add", {x, y});
  EXPECT_TRUE(FilterTest({x, y, z->make_array()}, SsaOperation::kAddWithoutChecks, SsaOperation::kEqual) == 3);
}

TEST(ProgramStep, Substract) {
  auto x = NumVecToArray<int>(arrow::int32(), {10, 34, 8});
  auto y = NumVecToArray<int>(arrow::int32(), {32, 12, 4});
  auto z = arrow::compute::CallFunction("subtract", {x, y});
  EXPECT_TRUE(FilterTest({x, y, z->make_array()}, SsaOperation::kSubtractWithoutChecks, SsaOperation::kEqual) == 3);
}

TEST(ProgramStep, Multiply) {
  auto x = NumVecToArray<int>(arrow::int32(), {10, 34, 8});
  auto y = NumVecToArray<int>(arrow::int32(), {32, 12, 4});
  auto z = arrow::compute::CallFunction("multiply", {x, y});
  EXPECT_TRUE(FilterTest({x, y, z->make_array()}, SsaOperation::kMultiplyWithoutChecks, SsaOperation::kEqual) == 3);
}

TEST(ProgramStep, Divide) {
  auto x = NumVecToArray<int>(arrow::int32(), {10, 34, 8});
  auto y = NumVecToArray<int>(arrow::int32(), {32, 12, 4});
  auto z = arrow::compute::CallFunction("divide", {x, y});
  EXPECT_TRUE(FilterTest({x, y, z->make_array()}, SsaOperation::kDivideWithoutChecks, SsaOperation::kEqual) == 3);
}

TEST(ProgramStep, Abs) {
  auto x = NumVecToArray<int>(arrow::int32(), {-64, -16, 8});
  auto z = arrow::compute::CallFunction("abs", {x});
  EXPECT_TRUE(FilterTestUnary({x, z->make_array()}, SsaOperation::kAbsolute, SsaOperation::kEqual) == 3);
}

TEST(ProgramStep, Negate) {
  auto x = NumVecToArray<int>(arrow::int32(), {-64, -16, 8});
  auto z = arrow::compute::CallFunction("negate", {x});
  EXPECT_TRUE(FilterTestUnary({x, z->make_array()}, SsaOperation::kNegative, SsaOperation::kEqual) == 3);
}

TEST(ProgramStep, Compares) {
  for (auto eop : {SsaOperation::kEqual, SsaOperation::kLessThan, SsaOperation::kGreaterThan,
                   SsaOperation::kGreaterThanOrEqualTo, SsaOperation::kLessThanOrEqualTo, SsaOperation::kNotEqual}) {
    auto x = NumVecToArray<int>(arrow::int32(), {64, 5, 1});
    auto y = NumVecToArray<int>(arrow::int32(), {64, 1, 5});
    auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x, y});
    EXPECT_TRUE(FilterTest({x, y, z->make_array()}, eop, SsaOperation::kEqual) == 3);
  }
}

TEST(ProgramStep, Logic0) {
  for (auto eop : {SsaOperation::kBitwiseAnd, SsaOperation::kBitwiseOr, SsaOperation::kXor}) {
    auto x = BoolVecToArray({true, false, false});
    auto y = BoolVecToArray({true, true, false});
    auto z = arrow::compute::CallFunction(GetFunctionName(eop), {x, y});
    EXPECT_TRUE(FilterTest({x, y, z->make_array()}, eop, SsaOperation::kEqual) == 3);
  }
}

TEST(ProgramStep, Logic1) {
  auto x = BoolVecToArray({true, false, false});
  auto z = arrow::compute::CallFunction("invert", {x});
  EXPECT_TRUE(FilterTestUnary({x, z->make_array()}, SsaOperation::kBitwiseNot, SsaOperation::kEqual) == 3);
}

#if 0  // TODO
TEST(ProgramStep, MatchSubstring) {
  for (auto type : {arrow::utf8() /*, arrow::binary()*/}) {
    std::vector<bool> res = LikeTest({"aa", "abaaba", "baa", ""}, SsaOperation::kLike, "aa", type);
    EXPECT_EQ(res.size(), 4);
    EXPECT_EQ(res[0], true);
    EXPECT_EQ(res[1], true);
    EXPECT_EQ(res[2], true);
    EXPECT_EQ(res[3], false);
  }
}
#endif

TEST(ProgramStep, ScalarTest) {
  auto schema = std::make_shared<arrow::Schema>(std::vector{
      std::make_shared<arrow::Field>("x", arrow::int64()), std::make_shared<arrow::Field>("filter", arrow::boolean())});
  auto batch = arrow::RecordBatch::Make(
      schema, 4,
      std::vector{NumVecToArray<int>(arrow::int64(), {64, 5, 1, 43}), BoolVecToArray({true, false, false, true})});
  EXPECT_TRUE(batch->ValidateFull().ok());

  const std::string x = "x";
  const std::string y = "y";
  const std::string res = "res";
  const std::string filter = "filter";

  auto step = std::make_shared<ProgramStep>();
  step->assignes.emplace_back(Assign(y, 56));
  step->assignes.emplace_back(Assign(res, SsaOperation::kAddWithoutChecks, {x, y}));
  step->filters.emplace_back(filter);
  step->projection.emplace_back(filter);
  step->projection.emplace_back(res);
  auto status = ApplyProgram(batch, Program({step}));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
  }
  EXPECT_TRUE(batch->ValidateFull().ok());
  EXPECT_EQ(batch->num_columns(), 2);
  EXPECT_EQ(batch->num_rows(), 2);
}

TEST(ProgramStep, Projection) {
  auto schema = std::make_shared<arrow::Schema>(std::vector{std::make_shared<arrow::Field>("x", arrow::int64()),
                                                            std::make_shared<arrow::Field>("y", arrow::boolean())});
  auto batch = arrow::RecordBatch::Make(
      schema, 4,
      std::vector{NumVecToArray<int>(arrow::int64(), {64, 5, 1, 43}), BoolVecToArray({true, false, false, true})});
  EXPECT_TRUE(batch->ValidateFull().ok());

  const std::string x = "x";

  auto step = std::make_shared<ProgramStep>();
  step->projection.emplace_back(x);
  auto status = ApplyProgram(batch, Program({step}));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
  }
  EXPECT_TRUE(batch->ValidateFull().ok());
  EXPECT_EQ(batch->num_columns(), 1);
  EXPECT_EQ(batch->num_rows(), 4);
}

}  // namespace iceberg::compute
