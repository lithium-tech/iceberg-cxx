#include "iceberg/equality_delete/generic_delete.h"

#include <memory>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "iceberg/equality_delete/utils.h"
#include "iceberg/test_utils/arrow_array.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/optional_vector.h"

namespace iceberg {
namespace {

TEST(GenericDeleteTest, Int16) {
  GenericEqualityDelete del(std::make_shared<MemoryState>(std::nullopt, false));
  auto del_array = CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{1, 2});
  ASSERT_OK(del.Add({del_array}, 2));
  EXPECT_EQ(del.Size(), 2);

  auto check_array = CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{std::nullopt, 1, 2, 12});
  EXPECT_EQ(del.IsDeleted({check_array}, 0), false);
  EXPECT_EQ(del.IsDeleted({check_array}, 1), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 2), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 3), false);
}

TEST(GenericDeleteTest, Null) {
  GenericEqualityDelete del(std::make_shared<MemoryState>(std::nullopt, false));
  auto del_array = CreateArray<arrow::Int32Builder>(OptionalVector<int32_t>{std::nullopt});
  ASSERT_OK(del.Add({del_array}, 1));
  EXPECT_EQ(del.Size(), 1);

  auto check_array = CreateArray<arrow::Int32Builder>(OptionalVector<int32_t>{std::nullopt, 1, 2, 12});
  EXPECT_EQ(del.IsDeleted({check_array}, 0), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 1), false);
  EXPECT_EQ(del.IsDeleted({check_array}, 2), false);
  EXPECT_EQ(del.IsDeleted({check_array}, 3), false);
}

TEST(SpecializedMultipleColumnDelete, Nulls) {
  auto del_array1 = CreateArray<arrow::Int32Builder>(OptionalVector<int16_t>{-1, std::nullopt, std::nullopt});
  auto del_array2 = CreateArray<arrow::Int32Builder>(OptionalVector<int16_t>{std::nullopt, -2, std::nullopt});

  GenericEqualityDelete del(std::make_shared<MemoryState>(std::nullopt, false));

  ASSERT_OK(del.Add({del_array1, del_array2}, 3));
  EXPECT_EQ(del.Size(), 3);

  auto check_array1 = CreateArray<arrow::Int32Builder>(
      OptionalVector<int32_t>{std::nullopt, -2, 1, -1, std::nullopt, 2, std::nullopt, 2, std::nullopt});
  auto check_array2 = CreateArray<arrow::Int32Builder>(
      OptionalVector<int32_t>{-1, std::nullopt, 1, std::nullopt, -2, 2, 2, std::nullopt, std::nullopt});
  const auto& check_arrays = std::vector{check_array1, check_array2};
  EXPECT_EQ(del.IsDeleted(check_arrays, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 1), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 2), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 3), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 4), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 5), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 6), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 7), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 8), true);
}

TEST(SpecializedMultipleColumnDelete, VariableLength) {
  auto del_array1 =
      CreateArray<arrow::BinaryBuilder>(OptionalVector<std::string>{std::nullopt, "a", "b", std::string("\0 ", 2)});
  GenericEqualityDelete del(std::make_shared<MemoryState>(std::nullopt, false));

  ASSERT_OK(del.Add({del_array1}, 4));
  EXPECT_EQ(del.Size(), 4);

  auto check_array1 = CreateArray<arrow::BinaryBuilder>(
      OptionalVector<std::string>{std::nullopt, "a", "b", "a ", "", std::string("\0 ", 2), " ", std::string(" \0", 2)});
  const auto& check_arrays = std::vector{check_array1};
  EXPECT_EQ(del.IsDeleted(check_arrays, 0), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 1), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 2), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 3), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 4), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 5), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 6), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 7), false);
}

TEST(SpecializedMultipleColumnDelete, VariableLengthMultipleColumns) {
  auto del_array1 = CreateArray<arrow::StringBuilder>(OptionalVector<std::string>{"xy"});
  auto del_array2 = CreateArray<arrow::StringBuilder>(OptionalVector<std::string>{"z"});

  GenericEqualityDelete del(std::make_shared<MemoryState>(std::nullopt, false));

  ASSERT_OK(del.Add({del_array1, del_array2}, 1));
  EXPECT_EQ(del.Size(), 1);

  auto check_array1 = CreateArray<arrow::StringBuilder>(OptionalVector<std::string>{"xy", "x"});
  auto check_array2 = CreateArray<arrow::StringBuilder>(OptionalVector<std::string>{"z", "yz"});

  const auto& check_arrays = std::vector{check_array1, check_array2};
  EXPECT_EQ(del.IsDeleted(check_arrays, 0), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 1), false);
}

}  // namespace
}  // namespace iceberg
