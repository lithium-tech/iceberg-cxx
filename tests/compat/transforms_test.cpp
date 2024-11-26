#include "iceberg/transforms.h"

#include <arrow/filesystem/localfs.h>
#include <arrow/io/file.h>
#include <arrow/scalar.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>

#include <limits>
#include <memory>
#include <stdexcept>

#include "gtest/gtest.h"
#include "iceberg/schema.h"
#include "iceberg/sql_catalog.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

TEST(Transforms, All) {
  auto num_scalar = std::make_shared<arrow::Int32Scalar>(87654321);
  int64_t milliseconds_since_epoch = 1704067200000;
  // Date: 2024-01-01
  auto date_scalar = std::make_shared<arrow::Date64Scalar>(milliseconds_since_epoch);

  {
    auto transform = IdentityTransform();
    auto transformed_value = std::static_pointer_cast<arrow::Int32Scalar>(transform.Transform(num_scalar));
    EXPECT_EQ(transformed_value->value, 87654321);
  }

#ifdef USE_SMHASHER
  {
    auto transform = BucketTransform(12345678);
    auto transformed_value = std::static_pointer_cast<arrow::Int32Scalar>(transform.Transform(num_scalar));
    EXPECT_EQ(transformed_value->value, 4250729);
  }

  {
    auto transform = TruncateTransform(3);
    auto transformed_value = std::static_pointer_cast<arrow::Int32Scalar>(transform.Transform(num_scalar));
    EXPECT_EQ(transformed_value->value, -1612368767);
  }
#endif

  {
    auto transform = MonthTransform();
    auto transformed_value = std::static_pointer_cast<arrow::Int32Scalar>(transform.Transform(date_scalar));
    EXPECT_EQ(transformed_value->value, 1);
  }

  {
    auto transform = DayTransform();
    auto transformed_value = std::static_pointer_cast<arrow::Int32Scalar>(transform.Transform(date_scalar));
    EXPECT_EQ(transformed_value->value, 1);
  }

  {
    auto transform = YearTransform();
    auto transformed_value = std::static_pointer_cast<arrow::Int32Scalar>(transform.Transform(date_scalar));
    EXPECT_EQ(transformed_value->value, 2024);
  }
}

}  // namespace iceberg
