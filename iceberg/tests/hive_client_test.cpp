#include "iceberg/src/hive_client.h"

#include <string>

#include "gtest/gtest.h"

namespace iceberg {

#ifdef USE_ICEBERG
TEST(HiveClient, Test) {
  HiveClient hive_client("127.0.0.1", 9090);
  const std::string metadata_location =
      hive_client.GetMetadataLocation("gperov", "test");
  ASSERT_EQ(metadata_location,
            "s3://warehouse/gperov/test/metadata/"
            "00003-aaa5649c-d0a0-4bdd-bf89-1a63bba01b37.metadata.json");
}
#endif

}  // namespace iceberg
