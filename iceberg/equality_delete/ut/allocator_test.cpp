#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "iceberg/equality_delete/utils.h"

namespace iceberg {
namespace {

TEST(LimitedAllocator, Simple) {
  auto alloc = iceberg::LimitedAllocator<int32_t>(std::make_shared<iceberg::MemoryState>(10, true));
  alloc.allocate(1);
  alloc.allocate(1);
  EXPECT_ANY_THROW(alloc.allocate(1));
}

TEST(LimitedAllocator, Vector) {
  auto alloc = iceberg::LimitedAllocator<int32_t>(std::make_shared<iceberg::MemoryState>(10, true));
  std::vector<int32_t, iceberg::LimitedAllocator<int32_t>> v(alloc);

  v.push_back(1);
  // This push_back will allocate 8 bytes due reallocation, so it should fail.
  EXPECT_ANY_THROW(v.push_back(1));
}

}  // namespace
}  // namespace iceberg
