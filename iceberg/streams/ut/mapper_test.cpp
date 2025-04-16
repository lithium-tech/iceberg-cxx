#include "iceberg/streams/iceberg/mapper.h"

#include "gtest/gtest.h"

namespace iceberg {
namespace {

TEST(MapperTest, Trivial) {
  std::map<int, std::string> map{{1, "first"}, {2, "second"}};

  FieldIdMapper mapper(std::move(map));
  EXPECT_EQ(mapper.FieldIdToColumnName(1), "first");
  EXPECT_EQ(mapper.FieldIdToColumnName(2), "second");
}

TEST(MapperTest, NonExistingField) {
  std::map<int, std::string> map{{1, "first"}, {2, "second"}};

  FieldIdMapper mapper(std::move(map));
  EXPECT_EQ(mapper.FieldIdToColumnName(3), "__unnamed_c0_f3");
}

TEST(MapperTest, NonExistingFieldExistingName) {
  std::map<int, std::string> map{{1, "__unnamed_c1_f3"}, {2, "__unnamed_c0_f3"}};

  FieldIdMapper mapper(std::move(map));
  EXPECT_EQ(mapper.FieldIdToColumnName(3), "__unnamed_c2_f3");
}

}  // namespace
}  // namespace iceberg
