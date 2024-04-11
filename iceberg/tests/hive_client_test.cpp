#include <string>

#include "gtest/gtest.h"
#include "iceberg/src/tea_hive_catalog.h"

namespace iceberg {

#ifdef USE_ICEBERG
TEST(HiveCatalog, Test) {
  ice_tea::HiveCatalog hive_client("127.0.0.1", 9090);
  auto table = hive_client.LoadTable(catalog::TableIdentifier{.db = "gperov", .name = "test"});
  ASSERT_TRUE(!!table);
  ASSERT_EQ(table->Location(),
            "s3://warehouse/gperov/test/metadata/"
            "00003-aaa5649c-d0a0-4bdd-bf89-1a63bba01b37.metadata.json");
}
#endif

}  // namespace iceberg
