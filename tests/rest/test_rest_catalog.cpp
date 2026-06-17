#include "gtest/gtest.h"
#include "iceberg/tea_rest_catalog.h"

namespace iceberg {

TEST(Catalog, REST) {
  auto c = ice_tea::RESTCatalog("http://localhost:8181/catalog", "91dc12d2-534d-11f1-9109-73b91866a831");

  auto table_identifier = catalog::TableIdentifier{.db = "ns1", .name = "lineitem"};

  EXPECT_EQ(c.LoadTable(table_identifier)->Location(),
            "s3://bucket1/tpch/sf1000/lineitem-39d2e5975c0d4a42ae0c4b74409cb6ec/metadata/"
            "00002-b5453916-aeea-4890-bbfd-9966f498dda0.metadata.json");
}

}  // namespace iceberg
