#include <memory>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "iceberg/equality_delete/specialized_multiple_column_delete.h"
#include "iceberg/equality_delete/specialized_one_column_delete.h"
#include "iceberg/equality_delete/utils.h"
#include "iceberg/test_utils/arrow_array.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/optional_vector.h"

namespace iceberg {
namespace {

TEST(SpecializedInt16Delete, SanityCheck) {
  SpecializedDeleteOneColumn<arrow::Int16Array> del(arrow::Type::INT16,
                                                    std::make_shared<MemoryState>(std::nullopt, false));
  auto del_array = CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{1, 2});
  ASSERT_OK(del.Add({del_array}, 2, 0));
  EXPECT_EQ(del.Size(), 2);

  auto check_array = CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{std::nullopt, 1, 2, 12});
  EXPECT_EQ(del.IsDeleted({check_array}, 0, 0), false);
  EXPECT_EQ(del.IsDeleted({check_array}, 1, 0), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 2, 0), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 3, 0), false);
}

TEST(SpecializedInt32Delete, SanityCheck) {
  SpecializedDeleteOneColumn<arrow::Int32Array> del(arrow::Type::INT32,
                                                    std::make_shared<MemoryState>(std::nullopt, false));
  auto del_array = CreateArray<arrow::Int32Builder>(OptionalVector<int32_t>{1, 2});
  ASSERT_OK(del.Add({del_array}, 2, 0));
  EXPECT_EQ(del.Size(), 2);

  auto check_array = CreateArray<arrow::Int32Builder>(OptionalVector<int32_t>{std::nullopt, 1, 2, 12});
  EXPECT_EQ(del.IsDeleted({check_array}, 0, 0), false);
  EXPECT_EQ(del.IsDeleted({check_array}, 1, 0), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 2, 0), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 3, 0), false);
}

TEST(SpecializedInt64Delete, SanityCheck) {
  SpecializedDeleteOneColumn<arrow::Int64Array> del(arrow::Type::INT64,
                                                    std::make_shared<MemoryState>(std::nullopt, false));
  auto del_array = CreateArray<arrow::Int64Builder>(OptionalVector<int64_t>{1, 2});
  ASSERT_OK(del.Add({del_array}, 2, 0));
  EXPECT_EQ(del.Size(), 2);

  auto check_array = CreateArray<arrow::Int64Builder>(OptionalVector<int64_t>{std::nullopt, 1, 2, 12});
  EXPECT_EQ(del.IsDeleted({check_array}, 0, 0), false);
  EXPECT_EQ(del.IsDeleted({check_array}, 1, 0), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 2, 0), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 3, 0), false);
}

TEST(SpecializedTimestampDelete, SanityCheck) {
  SpecializedDeleteOneColumn<arrow::TimestampArray> del(arrow::Type::TIMESTAMP,
                                                        std::make_shared<MemoryState>(std::nullopt, false));
  auto del_array = CreateArray<arrow::TimestampArray>(OptionalVector<int64_t>{1, 2});
  ASSERT_OK(del.Add({del_array}, 2, 0));
  EXPECT_EQ(del.Size(), 2);

  auto check_array = CreateArray<arrow::TimestampArray>(OptionalVector<int64_t>{std::nullopt, 1, 2, 12});
  EXPECT_EQ(del.IsDeleted({check_array}, 0, 0), false);
  EXPECT_EQ(del.IsDeleted({check_array}, 1, 0), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 2, 0), true);
  EXPECT_EQ(del.IsDeleted({check_array}, 3, 0), false);
}

TEST(SpecializedMultipleColumnDelete, SanityCheck) {
  auto array1 = CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{-1, 2});
  auto array2 = CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{3, -1});

  SpecializedDeleteMultipleColumn<int32_t> del({arrow::Type::INT16, arrow::Type::INT16}, {0, 1},
                                               std::make_shared<MemoryState>(std::nullopt, false));

  ASSERT_OK(del.Add({array1, array2}, 2, 0));
  EXPECT_EQ(del.Size(), 2);

  auto check_array1 =
      CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{1, -1, 2, 2, std::nullopt, 2, std::nullopt});
  auto check_array2 =
      CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{1, 3, -1, 2, 2, std::nullopt, std::nullopt});
  const auto& check_arrays = std::vector{check_array1, check_array2};
  EXPECT_EQ(del.IsDeleted(check_arrays, 0, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 1, 0), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 2, 0), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 3, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 4, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 5, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 6, 0), false);
}

TEST(SpecializedMultipleColumnDelete, NonTrivialOrder) {
  auto array1 = CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{-1, 2});
  auto array2 = CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{3, -1});

  SpecializedDeleteMultipleColumn<int32_t> del({arrow::Type::INT16, arrow::Type::INT16}, {1, 0},
                                               std::make_shared<MemoryState>(std::nullopt, false));

  ASSERT_OK(del.Add({array1, array2}, 2, 0));
  EXPECT_EQ(del.Size(), 2);

  auto check_array1 =
      CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{3, 1, 1, -1, 2, 2, std::nullopt, 2, std::nullopt});
  auto check_array2 =
      CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{2, 1, 1, 3, -1, 2, 2, std::nullopt, std::nullopt});
  const auto& check_arrays = std::vector{check_array1, check_array2};
  EXPECT_EQ(del.IsDeleted(check_arrays, 0, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 1, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 2, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 3, 0), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 4, 0), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 5, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 6, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 7, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 8, 0), false);
}

TEST(SpecializedMultipleColumnDelete, Nulls) {
  auto array1 = CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{-1, std::nullopt, std::nullopt});
  auto array2 = CreateArray<arrow::Int16Builder>(OptionalVector<int16_t>{std::nullopt, -2, std::nullopt});

  SpecializedDeleteMultipleColumn<int32_t> del({arrow::Type::INT16, arrow::Type::INT16}, {0, 1},
                                               std::make_shared<MemoryState>(std::nullopt, false));

  ASSERT_OK(del.Add({array1, array2}, 3, 0));
  EXPECT_EQ(del.Size(), 3);

  auto check_array1 = CreateArray<arrow::Int16Builder>(
      OptionalVector<int16_t>{std::nullopt, -2, 1, -1, std::nullopt, 2, std::nullopt, 2, std::nullopt});
  auto check_array2 = CreateArray<arrow::Int16Builder>(
      OptionalVector<int16_t>{-1, std::nullopt, 1, std::nullopt, -2, 2, 2, std::nullopt, std::nullopt});
  const auto& check_arrays = std::vector{check_array1, check_array2};
  EXPECT_EQ(del.IsDeleted(check_arrays, 0, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 1, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 2, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 3, 0), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 4, 0), true);
  EXPECT_EQ(del.IsDeleted(check_arrays, 5, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 6, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 7, 0), false);
  EXPECT_EQ(del.IsDeleted(check_arrays, 8, 0), true);
}

}  // namespace
}  // namespace iceberg
