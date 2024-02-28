#include "iceberg/src/hive_client.h"

#include <string>

#include "gtest/gtest.h"

namespace iceberg {

// TODO(gmusya)
#ifdef ICEBERG_LOCAL_TESTS
TEST(HiveClient, Test) {
  HiveClient hive_client("localhost", 9083);
  const std::string metadata_location =
      hive_client.GetMetadataLocation("user", "bigcat");
  ASSERT_EQ(
      metadata_location,
      "s3://datalake/user.db/bigcat-51d602c06af049c3888ebe678b5ebe15/"
      "metadata/00004-793695fb-d629-4855-8990-0efea814203a.metadata.json");
}
#endif

}  // namespace iceberg
