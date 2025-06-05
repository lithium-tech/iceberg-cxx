#include "iceberg/filter/stats_filter/stats_filter.h"

#include <optional>
#include <stdexcept>
#include <unordered_map>

#include "gtest/gtest.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/representation/value.h"
#include "iceberg/filter/stats_filter/stats.h"
#include "iceberg/test_utils/assertions.h"

namespace iceberg::filter {

using StatsContainer = std::unordered_map<std::string, GenericStats>;

class SimpleStatsGetter : public IStatsGetter {
 public:
  std::optional<GenericStats> GetStats(const std::string& column_name, filter::ValueType value_type) const override {
    auto it = stats.find(column_name);
    if (it == stats.end()) {
      return std::nullopt;
    }

    const auto& possible_stats = it->second;
    if (possible_stats.min_max.GetValueType() != value_type) {
      return std::nullopt;
    }
    return possible_stats;
  }

  StatsContainer stats;
};

using iceberg::filter::ValueType::kBool;
using iceberg::filter::ValueType::kDate;
using iceberg::filter::ValueType::kFloat4;
using iceberg::filter::ValueType::kFloat8;
using iceberg::filter::ValueType::kInt2;
using iceberg::filter::ValueType::kInt4;
using iceberg::filter::ValueType::kInt8;
using iceberg::filter::ValueType::kInterval;
using iceberg::filter::ValueType::kString;
using iceberg::filter::ValueType::kTime;
using iceberg::filter::ValueType::kTimestamp;
using iceberg::filter::ValueType::kTimestamptz;

template <typename Comparator>
MatchedRows Eval(size_t lmin, size_t lmax, size_t rmin, size_t rmax) {
  Comparator cmp;

  bool all_match = true;
  bool none_match = true;

  for (size_t l = lmin; l <= lmax; ++l) {
    for (size_t r = rmin; r <= rmax; ++r) {
      if (cmp(l, r)) {
        none_match = false;
      } else {
        all_match = false;
      }
    }
  }

  if (all_match) {
    return MatchedRows::kAll;
  } else if (none_match) {
    return MatchedRows::kNone;
  }
  return MatchedRows::kSome;
}

MatchedRows Eval(size_t lmin, size_t lmax, size_t rmin, size_t rmax, FunctionID comparator) {
  switch (comparator) {
    case FunctionID::kLessThan:
      return Eval<std::less<size_t>>(lmin, lmax, rmin, rmax);
    case FunctionID::kGreaterThan:
      return Eval<std::greater<size_t>>(lmin, lmax, rmin, rmax);
    case FunctionID::kLessThanOrEqualTo:
      return Eval<std::less_equal<size_t>>(lmin, lmax, rmin, rmax);
    case FunctionID::kGreaterThanOrEqualTo:
      return Eval<std::greater_equal<size_t>>(lmin, lmax, rmin, rmax);
    case FunctionID::kEqual:
      return Eval<std::equal_to<size_t>>(lmin, lmax, rmin, rmax);
    case FunctionID::kNotEqual:
      return Eval<std::not_equal_to<size_t>>(lmin, lmax, rmin, rmax);
    default:
      throw std::runtime_error("Bad comparator id: " + std::to_string(static_cast<int>(comparator)));
  }
}

template <ValueType lhs_value_type, ValueType rhs_value_type>
void TestDifferentTypes(const std::vector<PhysicalType<lhs_value_type>>& l_arr,
                        const std::vector<PhysicalType<rhs_value_type>>& r_arr) {
  for (size_t lmin = 0; lmin < l_arr.size(); ++lmin) {
    for (size_t lmax = lmin; lmax < r_arr.size(); ++lmax) {
      for (size_t rmin = 0; rmin < l_arr.size(); ++rmin) {
        for (size_t rmax = rmin; rmax < r_arr.size(); ++rmax) {
          SimpleStatsGetter simple_stats_getter;
          StatsContainer& stats = simple_stats_getter.stats;

          auto lhs = std::make_shared<VariableNode>(lhs_value_type, "lhs");
          auto rhs = std::make_shared<VariableNode>(rhs_value_type, "rhs");

          auto lhs_stats = GenericMinMaxStats::Make<lhs_value_type>(l_arr[lmin], l_arr[lmax]);
          auto rhs_stats = GenericMinMaxStats::Make<rhs_value_type>(r_arr[rmin], r_arr[rmax]);

          stats.emplace("lhs", std::move(lhs_stats));
          stats.emplace("rhs", std::move(rhs_stats));

          for (const auto& func_id : {FunctionID::kLessThan, FunctionID::kGreaterThan, FunctionID::kLessThanOrEqualTo,
                                      FunctionID::kGreaterThanOrEqualTo, FunctionID::kEqual, FunctionID::kNotEqual}) {
            MatchedRows expected_result = Eval(lmin, lmax, rmin, rmax, func_id);

            auto root_node = std::make_shared<filter::FunctionNode>(
                FunctionSignature{.function_id = func_id,
                                  .return_type = ValueType::kBool,
                                  .argument_types = std::vector<ValueType>{lhs_value_type, rhs_value_type}},
                std::vector<NodePtr>{lhs, rhs});

            StatsFilter filter(root_node, StatsFilter::Settings{});
            auto result = filter.ApplyFilter(simple_stats_getter);

            if (result != expected_result) {
              FAIL() << "lmin = " << lmin << ", lmax = " << lmax << ", rmin = " << rmin << ", rmax = " << rmax
                     << ", func_id = " << std::to_string(static_cast<int>(func_id)) << "\n"
                     << "Expected result: " << std::to_string(static_cast<int>(expected_result)) << "\n"
                     << "Actual result: " << std::to_string(static_cast<int>(result));
            }
          }
        }
      }
    }
  }
}

template <ValueType value_type>
void TestSameType(const std::vector<PhysicalType<value_type>>& values) {
  TestDifferentTypes<value_type, value_type>(values, values);
}

TEST(StatsFilter, CompareVariableBool) { TestSameType<kBool>(std::vector<PhysicalType<kBool>>{false, true}); }

template <typename T>
class StatsFilterIntegerTest : public ::testing::Test {};

using IntegerTypes =
    ::testing::Types<Tag<kInt2>, Tag<kInt4>, Tag<kInt8>, Tag<kTime>, Tag<kTimestamp>, Tag<kTimestamptz>, Tag<kDate>>;
TYPED_TEST_SUITE(StatsFilterIntegerTest, IntegerTypes);

TYPED_TEST(StatsFilterIntegerTest, Test) {
  constexpr ValueType value_type = TypeParam::value_type;
  TestSameType<value_type>(std::vector<PhysicalType<value_type>>{std::numeric_limits<PhysicalType<value_type>>::min(),
                                                                 -1, 0, 1, 2,
                                                                 std::numeric_limits<PhysicalType<value_type>>::max()});
}

template <typename T>
class StatsFilterIntegerDifferentTypesTest : public ::testing::Test {};

using IntegerDifferentTypes = ::testing::Types<std::pair<Tag<kInt2>, Tag<kInt4>>, std::pair<Tag<kInt2>, Tag<kInt8>>,
                                               std::pair<Tag<kInt4>, Tag<kInt8>>, std::pair<Tag<kInt4>, Tag<kInt2>>,
                                               std::pair<Tag<kInt8>, Tag<kInt2>>, std::pair<Tag<kInt8>, Tag<kInt4>>>;
TYPED_TEST_SUITE(StatsFilterIntegerDifferentTypesTest, IntegerDifferentTypes);

TYPED_TEST(StatsFilterIntegerDifferentTypesTest, Test) {
  constexpr ValueType lhs_value_type = TypeParam::first_type::value_type;
  constexpr ValueType rhs_value_type = TypeParam::second_type::value_type;

  TestDifferentTypes<lhs_value_type, rhs_value_type>(std::vector<PhysicalType<lhs_value_type>>{-1, 0, 1, 2},
                                                     std::vector<PhysicalType<rhs_value_type>>{-1, 0, 1, 2});
}

TEST(StatsFilter, CompareVariableString) {
  TestSameType<kString>(std::vector<PhysicalType<kString>>{"", "aa", "abcd", "z"});
}

template <typename T>
class StatsFilterFloatingPointTest : public ::testing::Test {};

using FloatingPointTypes = ::testing::Types<Tag<kFloat4>, Tag<kFloat8>>;
TYPED_TEST_SUITE(StatsFilterFloatingPointTest, FloatingPointTypes);

TYPED_TEST(StatsFilterFloatingPointTest, Test) {
  constexpr ValueType value_type = TypeParam::value_type;

  TestSameType<value_type>(
      std::vector<PhysicalType<value_type>>{-std::numeric_limits<PhysicalType<value_type>>::infinity(), -123, 0.0,
                                            21.01, std::numeric_limits<PhysicalType<value_type>>::infinity()});
}

TEST(StatsFilter, CheckIn) {
  auto lhs = std::make_shared<VariableNode>(kInt4, "lhs");
  auto array = ArrayHolder::Make<kInt4>(Array<kInt4>{2, 3, 4});

  auto root = std::make_shared<ScalarOverArrayFunctionNode>(
      FunctionSignature{.function_id = FunctionID::kEqual,
                        .return_type = kBool,
                        .argument_types = std::vector<ValueType>{kInt4, kInt4}},
      true, lhs, array);

  StatsFilter stats_filter(root, StatsFilter::Settings{});

  SimpleStatsGetter simple_stats_getter;
  StatsContainer& stats = simple_stats_getter.stats;

  auto lhs_stats = GenericMinMaxStats::Make<kInt4>(2, 2);

  stats.emplace("lhs", std::move(lhs_stats));

  EXPECT_EQ(stats_filter.ApplyFilter(simple_stats_getter), MatchedRows::kAll);
}

TEST(StatsFilter, CheckIsNull) {
  auto lhs = std::make_shared<VariableNode>(kInt4, "lhs");

  auto root_null = std::make_shared<FunctionNode>(
      FunctionSignature{
          .function_id = FunctionID::kIsNull, .return_type = kBool, .argument_types = std::vector<ValueType>{kInt4}},
      std::vector<NodePtr>{lhs});

  auto root_not_null = std::make_shared<LogicalNode>(LogicalNode::Operation::kNot, std::vector<NodePtr>{root_null});

  StatsFilter stats_filter_null(root_null, StatsFilter::Settings{});
  StatsFilter stats_filter_not_null(root_not_null, StatsFilter::Settings{});

  SimpleStatsGetter simple_stats_getter;
  StatsContainer& stats = simple_stats_getter.stats;

  auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(2, 2);

  {
    auto lhs_is_null_stats = CountStats{.contains_null = true, .contains_non_null = true};
    stats.emplace("lhs", GenericStats(lhs_minmax_stats, lhs_is_null_stats));
    EXPECT_EQ(stats_filter_null.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
    EXPECT_EQ(stats_filter_not_null.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
  }

  {
    stats.clear();
    auto lhs_is_null_stats = CountStats{.contains_null = true, .contains_non_null = false};
    stats.emplace("lhs", GenericStats(lhs_minmax_stats, lhs_is_null_stats));
    EXPECT_EQ(stats_filter_null.ApplyFilter(simple_stats_getter), MatchedRows::kAll);
    EXPECT_EQ(stats_filter_not_null.ApplyFilter(simple_stats_getter), MatchedRows::kNone);
  }

  {
    stats.clear();
    auto lhs_is_null_stats = CountStats{.contains_null = false, .contains_non_null = true};
    stats.emplace("lhs", GenericStats(lhs_minmax_stats, lhs_is_null_stats));
    EXPECT_EQ(stats_filter_null.ApplyFilter(simple_stats_getter), MatchedRows::kNone);
    EXPECT_EQ(stats_filter_not_null.ApplyFilter(simple_stats_getter), MatchedRows::kAll);
  }
}

TEST(StatsFilter, CheckLogical) {
  auto lhs = std::make_shared<VariableNode>(kInt4, "lhs");
  auto const2 = std::make_shared<ConstNode>(Value::Make<kInt4>(2));
  auto const4 = std::make_shared<ConstNode>(Value::Make<kInt4>(4));
  auto const6 = std::make_shared<ConstNode>(Value::Make<kInt4>(6));

  FunctionSignature cmp_signature{.function_id = FunctionID::kGreaterThan,
                                  .return_type = kBool,
                                  .argument_types = std::vector<ValueType>{kInt4, kInt4}};

  auto cmp2 = std::make_shared<FunctionNode>(cmp_signature, std::vector<NodePtr>{lhs, const2});
  auto cmp4 = std::make_shared<FunctionNode>(cmp_signature, std::vector<NodePtr>{lhs, const4});
  auto cmp6 = std::make_shared<FunctionNode>(cmp_signature, std::vector<NodePtr>{lhs, const6});

  auto and_node = std::make_shared<LogicalNode>(LogicalNode::Operation::kAnd, std::vector<NodePtr>{cmp2, cmp4, cmp6});
  auto or_node = std::make_shared<LogicalNode>(LogicalNode::Operation::kOr, std::vector<NodePtr>{cmp2, cmp4, cmp6});

  StatsFilter rg_filter_and(and_node, StatsFilter::Settings{});
  StatsFilter rg_filter_or(or_node, StatsFilter::Settings{});

  SimpleStatsGetter simple_stats_getter;
  StatsContainer& stats = simple_stats_getter.stats;

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(-5, -1);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter_and.ApplyFilter(simple_stats_getter), MatchedRows::kNone);
    EXPECT_EQ(rg_filter_or.ApplyFilter(simple_stats_getter), MatchedRows::kNone);
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(1, 6);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter_and.ApplyFilter(simple_stats_getter), MatchedRows::kNone);
    EXPECT_EQ(rg_filter_or.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(3, 10);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter_and.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
    EXPECT_EQ(rg_filter_or.ApplyFilter(simple_stats_getter), MatchedRows::kAll);
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(7, 12);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter_and.ApplyFilter(simple_stats_getter), MatchedRows::kAll);
    EXPECT_EQ(rg_filter_or.ApplyFilter(simple_stats_getter), MatchedRows::kAll);
  }
}

TEST(StatsFilter, CheckNot) {
  auto lhs = std::make_shared<VariableNode>(kInt4, "lhs");
  auto const2 = std::make_shared<ConstNode>(Value::Make<kInt4>(2));

  FunctionSignature cmp_signature{.function_id = FunctionID::kGreaterThan,
                                  .return_type = kBool,
                                  .argument_types = std::vector<ValueType>{kInt4, kInt4}};

  auto cmp2 = std::make_shared<FunctionNode>(cmp_signature, std::vector<NodePtr>{lhs, const2});
  auto not_cmp2 = std::make_shared<LogicalNode>(LogicalNode::Operation::kNot, std::vector<NodePtr>{cmp2});

  StatsFilter rg_filter(not_cmp2, StatsFilter::Settings{});

  SimpleStatsGetter simple_stats_getter;
  StatsContainer& stats = simple_stats_getter.stats;

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(-5, -1);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter.ApplyFilter(simple_stats_getter), MatchedRows::kAll);
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(-5, 15);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(3, 10);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter.ApplyFilter(simple_stats_getter), MatchedRows::kNone);
  }
}

TEST(StatsFilter, CheckComparisonDifferentTypes) {
  auto lhs = std::make_shared<VariableNode>(kInt2, "lhs");
  auto const2 = std::make_shared<ConstNode>(Value::Make<kInt4>(2));

  FunctionSignature cmp_signature{.function_id = FunctionID::kGreaterThan,
                                  .return_type = kBool,
                                  .argument_types = std::vector<ValueType>{kInt2, kInt4}};

  auto cmp2 = std::make_shared<FunctionNode>(cmp_signature, std::vector<NodePtr>{lhs, const2});

  StatsFilter rg_filter(cmp2, StatsFilter::Settings{});

  SimpleStatsGetter simple_stats_getter;
  StatsContainer& stats = simple_stats_getter.stats;

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt2>(-5, -1);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter.ApplyFilter(simple_stats_getter), MatchedRows::kNone);
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt2>(-5, 15);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt2>(3, 10);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter.ApplyFilter(simple_stats_getter), MatchedRows::kAll);
  }
}

TEST(StatsFilter, PartialFilter) {
  auto lhs = std::make_shared<VariableNode>(kInt4, "lhs");
  auto const2 = std::make_shared<ConstNode>(Value::Make<kInt4>(2));

  FunctionSignature cmp_signature{.function_id = FunctionID::kGreaterThan,
                                  .return_type = kBool,
                                  .argument_types = std::vector<ValueType>{kInt4, kInt4}};
  auto cmp2 = std::make_shared<FunctionNode>(cmp_signature, std::vector<NodePtr>{lhs, const2});

  FunctionSignature unsupported_signature{.function_id = FunctionID::kModuloWithChecks,
                                          .return_type = kInt4,
                                          .argument_types = std::vector<ValueType>{kInt4, kInt4}};
  auto unsupported_op = std::make_shared<FunctionNode>(unsupported_signature, std::vector<NodePtr>{lhs, const2});
  auto cmp3 = std::make_shared<FunctionNode>(cmp_signature, std::vector<NodePtr>{lhs, unsupported_op});

  auto or_op = std::make_shared<LogicalNode>(LogicalNode::Operation::kOr, std::vector<NodePtr>{cmp3, cmp2});
  auto and_op = std::make_shared<LogicalNode>(LogicalNode::Operation::kAnd, std::vector<NodePtr>{cmp3, cmp2});

  StatsFilter unsupported_cmp_filter(cmp3, StatsFilter::Settings{});
  StatsFilter or_filter(or_op, StatsFilter::Settings{});
  StatsFilter and_filter(and_op, StatsFilter::Settings{});

  SimpleStatsGetter simple_stats_getter;
  StatsContainer& stats = simple_stats_getter.stats;

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(3, 10);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(unsupported_cmp_filter.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
    EXPECT_EQ(or_filter.ApplyFilter(simple_stats_getter), MatchedRows::kAll);
    EXPECT_EQ(and_filter.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
  }
  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(-1, 1);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(unsupported_cmp_filter.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
    EXPECT_EQ(or_filter.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
    EXPECT_EQ(and_filter.ApplyFilter(simple_stats_getter), MatchedRows::kNone);
  }
}

TEST(StatsFilter, IfNode) {
  auto lhs = std::make_shared<VariableNode>(kInt4, "lhs");
  auto const2 = std::make_shared<ConstNode>(Value::Make<kInt4>(2));

  FunctionSignature cmp_signature{.function_id = FunctionID::kGreaterThan,
                                  .return_type = kBool,
                                  .argument_types = std::vector<ValueType>{kInt4, kInt4}};
  auto cmp2 = std::make_shared<FunctionNode>(cmp_signature, std::vector<NodePtr>{lhs, const2});

  auto then_value = std::make_shared<VariableNode>(kString, "then");
  auto else_value = std::make_shared<VariableNode>(kString, "else");

  auto if_node = std::make_shared<IfNode>(cmp2, then_value, else_value);

  StatsFilter rg_filter(if_node, StatsFilter::Settings{});

  SimpleStatsGetter simple_stats_getter;
  StatsContainer& stats = simple_stats_getter.stats;

  auto then_minmax_stats = GenericMinMaxStats::Make<kString>("a", "c");
  auto else_minmax_stats = GenericMinMaxStats::Make<kString>("b", "d");
  stats.emplace("then", GenericStats(then_minmax_stats));
  stats.emplace("else", GenericStats(else_minmax_stats));

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(-5, 10);

    stats.erase("lhs");
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));

    ASSIGN_OR_FAIL(auto res, rg_filter.Evaluate(simple_stats_getter));
    ASSERT_EQ(res.min_max.GetValueType(), kString);
    EXPECT_EQ(res.min_max.Get<kString>().min, "a");
    EXPECT_EQ(res.min_max.Get<kString>().max, "d");
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(10, 10);

    stats.erase("lhs");
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));

    ASSIGN_OR_FAIL(auto res, rg_filter.Evaluate(simple_stats_getter));
    ASSERT_EQ(res.min_max.GetValueType(), kString);
    EXPECT_EQ(res.min_max.Get<kString>().min, "a");
    EXPECT_EQ(res.min_max.Get<kString>().max, "c");
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kInt4>(-10, -10);

    stats.erase("lhs");
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));

    ASSIGN_OR_FAIL(auto res, rg_filter.Evaluate(simple_stats_getter));
    ASSERT_EQ(res.min_max.GetValueType(), kString);
    EXPECT_EQ(res.min_max.Get<kString>().min, "b");
    EXPECT_EQ(res.min_max.Get<kString>().max, "d");
  }
}

TEST(StatsFilter, CheckComparisonDateTimestamp) {
  const int64_t kMicrosInHalfDay = static_cast<int64_t>(24 * 60 * 60) * 1000 * 1000 / 2;

  auto lhs = std::make_shared<VariableNode>(kDate, "lhs");
  auto const2 = std::make_shared<ConstNode>(Value::Make<kTimestamp>(kMicrosInHalfDay));

  FunctionSignature cmp_signature{.function_id = FunctionID::kGreaterThan,
                                  .return_type = kBool,
                                  .argument_types = std::vector<ValueType>{kDate, kTimestamp}};

  auto cmp2 = std::make_shared<FunctionNode>(cmp_signature, std::vector<NodePtr>{lhs, const2});

  StatsFilter rg_filter(cmp2, StatsFilter::Settings{.timestamp_to_timestamptz_shift_us =
                                                        static_cast<int64_t>(3) * 60 * 60 * 1000 * 1000});

  SimpleStatsGetter simple_stats_getter;
  StatsContainer& stats = simple_stats_getter.stats;

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kDate>(-2, 0);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter.ApplyFilter(simple_stats_getter), MatchedRows::kNone);
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kDate>(0, 1);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter.ApplyFilter(simple_stats_getter), MatchedRows::kSome);
  }

  {
    auto lhs_minmax_stats = GenericMinMaxStats::Make<kDate>(1, 3);
    stats.clear();
    stats.emplace("lhs", GenericStats(lhs_minmax_stats));
    EXPECT_EQ(rg_filter.ApplyFilter(simple_stats_getter), MatchedRows::kAll);
  }
}

}  // namespace iceberg::filter
