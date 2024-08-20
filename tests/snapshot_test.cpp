#include <cctype>
#include <unordered_set>

#include "gtest/gtest.h"
#include "iceberg/uuid.h"

namespace iceberg {
namespace {

bool ValidateUUID(const std::string& uuid) {
  if (uuid.size() != 36) {
    return false;
  }

  static const std::unordered_set<size_t> special_indices{8, 13, 18, 23};
  for (auto ind : special_indices) {
    if (uuid[ind] != '-') {
      return false;
    }
  }

  for (size_t i = 0; i < uuid.size(); ++i) {
    if (special_indices.contains(i)) {
      continue;
    }
    if (!std::isalpha(uuid[i]) && !std::isdigit(uuid[i])) {
      return false;
    }
  }

  return true;
}

}  // namespace

TEST(UUID, Test) {
  Uuid null_uuid;
  EXPECT_TRUE(null_uuid.IsNull());
  EXPECT_EQ(null_uuid.ToString(), "00000000-0000-0000-0000-000000000000");
  EXPECT_TRUE(ValidateUUID(null_uuid.ToString()));

  std::string uuid_str1 = "61f0c404-5cb3-11e7-907b-a6006ad3dba0";
  EXPECT_TRUE(ValidateUUID(uuid_str1));

  Uuid uuid1(uuid_str1);
  EXPECT_FALSE(uuid1.IsNull());
  EXPECT_EQ(uuid1.ToString(), uuid_str1);

  EXPECT_LE(null_uuid, uuid1);
  EXPECT_LT(null_uuid, uuid1);
  EXPECT_GE(uuid1, null_uuid);
  EXPECT_GT(uuid1, null_uuid);
  EXPECT_NE(uuid1, null_uuid);
}

TEST(UUIDGenerator, Test) {
  UuidGenerator generator;

  size_t numUuids = 200;
  std::unordered_set<std::string> seen_uuids;

  for (size_t i = 0; i < numUuids; ++i) {
    auto generated = generator.CreateRandom();
    EXPECT_FALSE(generated.IsNull());

    auto uuid_str = generated.ToString();
    EXPECT_TRUE(ValidateUUID(uuid_str));

    EXPECT_FALSE(seen_uuids.contains(uuid_str));
    seen_uuids.insert(uuid_str);
  }
}

}  // namespace iceberg
