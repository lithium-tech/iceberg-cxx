#include <string>

#include "gtest/gtest.h"
#include "iceberg/src/tea_hive_catalog.h"

namespace iceberg {

#ifdef USE_ICEBERG
catalog::TableIdentifier GetTestTable();

TEST(HiveCatalog, Test) {
  ice_tea::HiveCatalog hive_client("127.0.0.1", 9090);
  auto table_ident = GetTestTable();
  auto table = hive_client.LoadTable(table_ident);
  std::string expected_path = "s3://warehouse/" + table_ident.db + "/" + table_ident.name;
  ASSERT_TRUE(!!table);
  ASSERT_EQ(table->Location(), expected_path + "/metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json");
}
#endif

}  // namespace iceberg
