#include "iceberg/filter/row_filter/extend_gandiva/arithmetic_ops.h"

#include <cstdint>

#include "gandiva/execution_context.h"
#include "gtest/gtest.h"

namespace iceberg {
namespace {

template <typename T>
class ArithmeticTest : public ::testing::Test {};

using SignedIntTypes = ::testing::Types<int16_t, int32_t, int64_t>;
TYPED_TEST_SUITE(ArithmeticTest, SignedIntTypes);

TYPED_TEST(ArithmeticTest, Add) {
  gandiva::ExecutionContext context;
  auto ctx = reinterpret_cast<int64_t>(&context);
  constexpr auto add_func = AddOverflow<TypeParam>;
  constexpr auto min_value = std::numeric_limits<TypeParam>::min();
  constexpr auto max_value = std::numeric_limits<TypeParam>::max();

  EXPECT_EQ(add_func(ctx, 2, 2), 4);
  EXPECT_EQ(add_func(ctx, max_value, min_value), -1);

  EXPECT_EQ(add_func(ctx, max_value, 1), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Overflow");
  context.Reset();

  EXPECT_EQ(add_func(ctx, min_value, -1), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Overflow");
  context.Reset();
}

TYPED_TEST(ArithmeticTest, Sub) {
  gandiva::ExecutionContext context;
  auto ctx = reinterpret_cast<int64_t>(&context);
  constexpr auto sub_func = SubOverflow<TypeParam>;
  constexpr auto min_value = std::numeric_limits<TypeParam>::min();
  constexpr auto max_value = std::numeric_limits<TypeParam>::max();

  EXPECT_EQ(sub_func(ctx, 4, 2), 2);
  EXPECT_EQ(sub_func(ctx, max_value, max_value - 12), 12);
  EXPECT_EQ(sub_func(ctx, min_value, min_value), 0);

  EXPECT_EQ(sub_func(ctx, max_value, -1), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Overflow");
  context.Reset();

  EXPECT_EQ(sub_func(ctx, min_value, 1), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Overflow");
  context.Reset();
}

TYPED_TEST(ArithmeticTest, Mul) {
  gandiva::ExecutionContext context;
  auto ctx = reinterpret_cast<int64_t>(&context);
  constexpr auto mul_func = MulOverflow<TypeParam>;
  constexpr auto min_value = std::numeric_limits<TypeParam>::min();
  constexpr auto max_value = std::numeric_limits<TypeParam>::max();

  EXPECT_EQ(mul_func(ctx, 2, 3), 6);
  EXPECT_EQ(mul_func(ctx, max_value, -1), min_value + 1);

  EXPECT_EQ(mul_func(ctx, min_value, -1), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Overflow");
  context.Reset();

  EXPECT_EQ(mul_func(ctx, max_value, 2), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Overflow");
  context.Reset();

  EXPECT_EQ(mul_func(ctx, min_value, 2), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Overflow");
  context.Reset();
}

TYPED_TEST(ArithmeticTest, Div) {
  gandiva::ExecutionContext context;
  auto ctx = reinterpret_cast<int64_t>(&context);
  constexpr auto div_func = DivSafe<TypeParam>;
  constexpr auto min_value = std::numeric_limits<TypeParam>::min();
  constexpr auto max_value = std::numeric_limits<TypeParam>::max();

  EXPECT_EQ(div_func(ctx, 6, 2), 3);
  EXPECT_EQ(div_func(ctx, max_value, -1), min_value + 1);

  EXPECT_EQ(div_func(ctx, min_value, 0), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Division by zero");
  context.Reset();

  EXPECT_EQ(div_func(ctx, max_value, 0), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Division by zero");
  context.Reset();

  EXPECT_EQ(div_func(ctx, min_value, -1), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Overflow");
  context.Reset();
}

TYPED_TEST(ArithmeticTest, Mod) {
  gandiva::ExecutionContext context;
  auto ctx = reinterpret_cast<int64_t>(&context);
  constexpr auto mod_func = ModSafe<TypeParam>;
  constexpr auto min_value = std::numeric_limits<TypeParam>::min();
  constexpr auto max_value = std::numeric_limits<TypeParam>::max();

  EXPECT_EQ(mod_func(ctx, 7, 3), 1);
  EXPECT_EQ(mod_func(ctx, max_value, -1), 0);
  EXPECT_TRUE(!context.has_error());
  EXPECT_EQ(mod_func(ctx, min_value, -1), 0);
  EXPECT_TRUE(!context.has_error());

  EXPECT_EQ(mod_func(ctx, min_value, 0), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Division by zero");
  context.Reset();

  EXPECT_EQ(mod_func(ctx, max_value, 0), 0);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "Division by zero");
  context.Reset();
}

TEST(BankersRound, Bankers) {
  EXPECT_EQ(BankersRound(-2.5), -2);
  EXPECT_EQ(BankersRound(-1.5), -2);
  EXPECT_EQ(BankersRound(-0.5), -0);
  EXPECT_EQ(BankersRound(0.5), 0);
  EXPECT_EQ(BankersRound(1.5), 2);
  EXPECT_EQ(BankersRound(2.5), 2);
  EXPECT_EQ(BankersRound(3.5), 4);
  EXPECT_EQ(BankersRound(4.5), 4);
}

}  // namespace
}  // namespace iceberg
